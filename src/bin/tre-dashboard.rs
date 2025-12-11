use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::{Frame, Terminal};
use reqwest::blocking::Client;
use tracing::{info, warn};

const DEFAULT_METRICS_URL: &str = "http://127.0.0.1:9898/metrics";
const DEFAULT_REFRESH_MS: u64 = 1_000;
const DEFAULT_HISTORY: usize = 60;
const PANEL_ENV: &str = "TRE_DASHBOARD_PANELS";
const HISTORY_ENV: &str = "TRE_DASHBOARD_HISTORY";
const INPUT_POLL_MS: u64 = 50;
const MAX_PANEL_COLUMNS: u16 = 3;
const BUNDLE_HISTORY_LIMIT: usize = 12;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let metrics_url =
        env::var("TRE_METRICS_URL").unwrap_or_else(|_| DEFAULT_METRICS_URL.to_string());
    let refresh_ms = env::var("TRE_DASHBOARD_REFRESH_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_REFRESH_MS)
        .max(250);
    let history_len = env::var(HISTORY_ENV)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_HISTORY)
        .max(8);
    let panels = PanelKind::from_env(env::var(PANEL_ENV).ok().as_deref());

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build metrics HTTP client")?;

    info!(
        target = "dashboard",
        url = %metrics_url,
        interval_ms = refresh_ms,
        history = history_len,
        panels = ?panels,
        "dashboard online"
    );
    enable_raw_mode().context("failed to enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    terminal.hide_cursor()?;

    let mut dashboard_state = DashboardState::new(history_len);
    let run_result = run_dashboard_loop(
        &mut terminal,
        &client,
        &metrics_url,
        Duration::from_millis(refresh_ms),
        &panels,
        &mut dashboard_state,
    );

    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();

    run_result
}

type DashboardTerminal = Terminal<CrosstermBackend<Stdout>>;

#[derive(Debug)]
struct MetricSample {
    labels: HashMap<String, String>,
    value: f64,
}

type MetricMap = HashMap<String, Vec<MetricSample>>;

fn run_dashboard_loop(
    terminal: &mut DashboardTerminal,
    client: &Client,
    metrics_url: &str,
    refresh: Duration,
    panel_kinds: &[PanelKind],
    state: &mut DashboardState,
) -> Result<()> {
    let mut metrics: MetricMap = HashMap::new();
    let mut last_updated: Option<Instant> = None;
    let mut last_error: Option<String> = None;

    loop {
        let iteration_start = Instant::now();
        match client.get(metrics_url).send() {
            Ok(resp) => match resp.text() {
                Ok(body) => {
                    metrics = parse_metrics(&body);
                    last_updated = Some(Instant::now());
                    if last_error.is_some() {
                        last_error = None;
                    }
                }
                Err(err) => {
                    warn!(target = "dashboard", "failed to read metrics body: {err}");
                    last_error = Some(format!("body error: {err}"));
                }
            },
            Err(err) => {
                warn!(target = "dashboard", "failed to fetch metrics: {err}");
                last_error = Some(format!("fetch error: {err}"));
            }
        }

        let now = Instant::now();
        state.refresh_bundle_events(&metrics, now);
        let panels = build_panels(&metrics, panel_kinds, state, now);
        let current_view = state.view();
        let error_snapshot = last_error.as_deref();
        terminal.draw(|frame| {
            draw_ui(
                frame,
                &panels,
                metrics_url,
                last_updated,
                error_snapshot,
                now,
                current_view,
            );
        })?;

        if wait_for_next_tick(iteration_start, refresh, state)? {
            break;
        }
    }

    Ok(())
}

fn parse_metrics(body: &str) -> MetricMap {
    let mut map: MetricMap = HashMap::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (metric_part, value_part) = match trimmed.rsplit_once(' ') {
            Some(parts) => parts,
            None => continue,
        };
        if metric_part.is_empty() {
            continue;
        }
        let value = match value_part.parse::<f64>() {
            Ok(value) => value,
            Err(_) => continue,
        };
        let (name, labels) = if let Some(start) = metric_part.find('{') {
            if let Some(end) = metric_part.rfind('}') {
                if end <= start {
                    continue;
                }
                let name = &metric_part[..start];
                let label_block = &metric_part[start + 1..end];
                (name.to_string(), parse_labels(label_block))
            } else {
                continue;
            }
        } else {
            (metric_part.to_string(), HashMap::new())
        };
        map.entry(name)
            .or_default()
            .push(MetricSample { labels, value });
    }
    map
}

fn parse_labels(block: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    if block.is_empty() {
        return labels;
    }
    for pair in block.split(',') {
        if let Some((key, raw_value)) = pair.split_once('=') {
            let value = raw_value.trim_matches('"').replace("\\\"", "\"");
            labels.insert(key.trim().to_string(), value);
        }
    }
    labels
}

#[derive(Debug)]
struct DashboardState {
    history: MetricHistory,
    counters: HashMap<String, CounterSample>,
    counter_totals: HashMap<String, f64>,
    bundle_events: VecDeque<BundleEvent>,
    last_bundle_hash: Option<i64>,
    last_bundle_hint: Option<String>,
    last_bundle_tip: Option<u64>,
    view: DashboardView,
}

impl DashboardState {
    fn new(history_len: usize) -> Self {
        Self {
            history: MetricHistory::new(history_len),
            counters: HashMap::new(),
            counter_totals: HashMap::new(),
            bundle_events: VecDeque::new(),
            last_bundle_hash: None,
            last_bundle_hint: None,
            last_bundle_tip: None,
            view: DashboardView::Bootstrap,
        }
    }

    fn record_history(&mut self, key: &str, value: f64) -> Option<String> {
        self.history.push(key, value);
        self.history.sparkline(key)
    }

    fn counter_rate(&mut self, key: &str, current: f64, now: Instant) -> Option<f64> {
        if !current.is_finite() {
            return None;
        }
        let entry = self
            .counters
            .entry(key.to_string())
            .or_insert_with(|| CounterSample {
                value: current,
                timestamp: now,
            });
        let elapsed = now.saturating_duration_since(entry.timestamp);
        if elapsed.as_secs_f64() <= 0.0 {
            entry.value = current;
            entry.timestamp = now;
            return None;
        }
        let delta = current - entry.value;
        entry.value = current;
        entry.timestamp = now;
        if delta >= 0.0 {
            Some(delta / elapsed.as_secs_f64())
        } else {
            None
        }
    }

    fn view(&self) -> DashboardView {
        self.view
    }

    fn set_view(&mut self, view: DashboardView) {
        self.view = view;
    }

    fn next_view(&mut self) {
        self.view = self.view.next();
    }

    fn previous_view(&mut self) {
        self.view = self.view.previous();
    }

    fn refresh_bundle_events(&mut self, metrics: &MetricMap, now: Instant) {
        if let Some(hash) = gauge_value(metrics, "bundle_execution_last_bundle_hash", &[]) {
            if hash.is_finite() {
                let hashed = hash.round() as i64;
                if Some(hashed) != self.last_bundle_hash {
                    self.last_bundle_hash = Some(hashed);
                    self.last_bundle_hint = Some(format_bundle_hash_hint(hashed));
                }
            }
        }
        if let Some(tip) = gauge_value(metrics, "bundle_execution_tip_lamports", &[]) {
            if tip.is_finite() && tip >= 0.0 {
                self.last_bundle_tip = Some(tip.round() as u64);
            }
        }

        self.record_bundle_attempt_events(metrics, now);
        self.record_bundle_result_events(metrics, now);
    }

    fn counter_delta_by_key(&mut self, key: &str, current: f64) -> u64 {
        if !current.is_finite() {
            return 0;
        }
        let entry = self
            .counter_totals
            .entry(key.to_string())
            .or_insert(current);
        let delta = (current - *entry).max(0.0);
        *entry = current;
        delta.round().max(0.0) as u64
    }

    fn record_bundle_attempt_events(&mut self, metrics: &MetricMap, now: Instant) {
        let Some(samples) = metrics.get("bundle_execution_attempts_total") else {
            return;
        };
        for sample in samples {
            let Some(status) = sample.labels.get("status").map(String::as_str) else {
                continue;
            };
            let key = metric_key("bundle_execution_attempts_total", &sample.labels);
            let delta = self.counter_delta_by_key(&key, sample.value);
            if delta == 0 {
                continue;
            }
            match status {
                "success" => self.append_bundle_events(BundleEventKind::Stage3Success, delta, now),
                "failure" => self.append_bundle_events(BundleEventKind::Stage3Failure, delta, now),
                _ => {}
            }
        }
    }

    fn record_bundle_result_events(&mut self, metrics: &MetricMap, now: Instant) {
        let Some(samples) = metrics.get("bundle_result_events_total") else {
            return;
        };
        for sample in samples {
            let Some(status) = sample.labels.get("status").map(String::as_str) else {
                continue;
            };
            let key = metric_key("bundle_result_events_total", &sample.labels);
            let delta = self.counter_delta_by_key(&key, sample.value);
            if delta == 0 {
                continue;
            }
            let kind = match status {
                "accepted" => Some(BundleEventKind::Accepted),
                "rejected" => Some(BundleEventKind::Rejected),
                "expired" => Some(BundleEventKind::Expired),
                "dropped" => Some(BundleEventKind::Dropped),
                "auction_won" => Some(BundleEventKind::AuctionWon),
                "auction_lost" => Some(BundleEventKind::AuctionLost),
                "processed" => Some(BundleEventKind::Processed),
                "finalized" => Some(BundleEventKind::Finalized),
                _ => None,
            };
            if let Some(kind) = kind {
                self.append_bundle_events(kind, delta, now);
            }
        }
    }

    fn append_bundle_events(&mut self, kind: BundleEventKind, count: u64, now: Instant) {
        for _ in 0..count {
            let tip_detail = if matches!(kind, BundleEventKind::Stage3Success) {
                self.last_bundle_tip
                    .map(|lamports| format!("{:.4} SOL", lamports as f64 / 1_000_000_000.0))
            } else {
                None
            };
            let event = BundleEvent {
                timestamp: now,
                kind,
                bundle_id: self.last_bundle_hint.clone(),
                detail: tip_detail.clone(),
            };
            self.bundle_events.push_back(event);
            while self.bundle_events.len() > BUNDLE_HISTORY_LIMIT {
                self.bundle_events.pop_front();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DashboardView {
    Bootstrap,
    Bundles,
}

impl DashboardView {
    const ALL: [DashboardView; 2] = [DashboardView::Bootstrap, DashboardView::Bundles];

    fn label(self) -> &'static str {
        match self {
            DashboardView::Bootstrap => "Bootstrap",
            DashboardView::Bundles => "Bundles",
        }
    }

    fn next(self) -> Self {
        let idx = Self::ALL
            .iter()
            .position(|candidate| *candidate == self)
            .unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    fn previous(self) -> Self {
        let idx = Self::ALL
            .iter()
            .position(|candidate| *candidate == self)
            .unwrap_or(0);
        if idx == 0 {
            *Self::ALL.last().unwrap()
        } else {
            Self::ALL[idx - 1]
        }
    }
}

#[derive(Debug)]
struct CounterSample {
    value: f64,
    timestamp: Instant,
}

#[derive(Debug)]
struct MetricHistory {
    capacity: usize,
    series: HashMap<String, VecDeque<f64>>,
}

#[derive(Clone, Debug)]
struct BundleEvent {
    timestamp: Instant,
    kind: BundleEventKind,
    bundle_id: Option<String>,
    detail: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum BundleEventKind {
    Stage3Success,
    Stage3Failure,
    Accepted,
    Rejected,
    Expired,
    Dropped,
    AuctionWon,
    AuctionLost,
    Processed,
    Finalized,
}

impl BundleEventKind {
    fn label(self) -> &'static str {
        match self {
            BundleEventKind::Stage3Success => "submitted",
            BundleEventKind::Stage3Failure => "failed",
            BundleEventKind::Accepted => "accepted",
            BundleEventKind::Rejected => "rejected",
            BundleEventKind::Expired => "expired",
            BundleEventKind::Dropped => "dropped",
            BundleEventKind::AuctionWon => "auction_won",
            BundleEventKind::AuctionLost => "auction_lost",
            BundleEventKind::Processed => "processed",
            BundleEventKind::Finalized => "finalized",
        }
    }
}

impl MetricHistory {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            series: HashMap::new(),
        }
    }

    fn push(&mut self, key: &str, value: f64) {
        if !value.is_finite() {
            return;
        }
        let entries = self
            .series
            .entry(key.to_string())
            .or_insert_with(VecDeque::new);
        if entries.len() == self.capacity {
            entries.pop_front();
        }
        entries.push_back(value);
    }

    fn sparkline(&self, key: &str) -> Option<String> {
        let entries = self.series.get(key)?;
        if entries.len() < 2 {
            return None;
        }
        Some(ascii_sparkline(entries.iter().copied().collect()))
    }
}

#[derive(Clone, Copy, Debug)]
enum PanelKind {
    Readiness,
    Planner,
    Rpc,
}

impl PanelKind {
    fn from_env(raw: Option<&str>) -> Vec<Self> {
        let default = "readiness,planner,rpc";
        let input = raw.unwrap_or(default);
        let mut selected: Vec<_> = input
            .split(',')
            .filter_map(|token| Self::from_token(token.trim()))
            .collect();
        if selected.is_empty() {
            selected.extend([Self::Readiness, Self::Planner, Self::Rpc]);
        }
        selected
    }

    fn from_token(token: &str) -> Option<Self> {
        match token.to_ascii_lowercase().as_str() {
            "readiness" => Some(Self::Readiness),
            "planner" | "submission" => Some(Self::Planner),
            "rpc" | "corecast" => Some(Self::Rpc),
            _ => None,
        }
    }
}

struct Panel {
    title: String,
    rows: Vec<PanelEntry>,
}

impl Panel {
    fn new(title: &str) -> Self {
        Self {
            title: title.to_string(),
            rows: Vec::new(),
        }
    }

    fn push(&mut self, label: impl Into<String>, value: impl Into<String>, spark: Option<String>) {
        self.push_styled(
            label,
            value,
            spark,
            Style::default().fg(Color::Gray),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    }

    fn push_styled(
        &mut self,
        label: impl Into<String>,
        value: impl Into<String>,
        spark: Option<String>,
        label_style: Style,
        value_style: Style,
    ) {
        self.rows.push(PanelEntry::Stat {
            label: label.into(),
            value: value.into(),
            spark,
            label_style,
            value_style,
        });
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

enum PanelEntry {
    Stat {
        label: String,
        value: String,
        spark: Option<String>,
        label_style: Style,
        value_style: Style,
    },
}

fn build_panels(
    metrics: &MetricMap,
    kinds: &[PanelKind],
    state: &mut DashboardState,
    now: Instant,
) -> Vec<Panel> {
    match state.view() {
        DashboardView::Bootstrap => build_bootstrap_panels(metrics, kinds, state, now),
        DashboardView::Bundles => build_bundle_panels(metrics, state, now),
    }
}

fn build_bootstrap_panels(
    metrics: &MetricMap,
    kinds: &[PanelKind],
    state: &mut DashboardState,
    now: Instant,
) -> Vec<Panel> {
    let mut output = Vec::new();
    for kind in kinds {
        match kind {
            PanelKind::Readiness => {
                if let Some(panel) = build_bootstrap_panel(metrics, state, now) {
                    output.push(panel);
                }
            }
            PanelKind::Planner => {
                if let Some(panel) = build_planner_panel(metrics, state, now) {
                    output.push(panel);
                }
            }
            PanelKind::Rpc => {
                if let Some(panel) = build_corecast_panel(metrics, state, now) {
                    output.push(panel);
                }
            }
        }
    }
    output
}

fn draw_ui(
    frame: &mut Frame<'_>,
    panels: &[Panel],
    metrics_url: &str,
    last_updated: Option<Instant>,
    last_error: Option<&str>,
    now: Instant,
    view: DashboardView,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Length(5), Constraint::Min(0)])
        .split(frame.size());

    let header = header_widget(metrics_url, last_updated, last_error, now, view);
    frame.render_widget(header, chunks[0]);

    if panels.is_empty() {
        let empty = Paragraph::new("No panels configured")
            .block(Block::default().title("Panels").borders(Borders::ALL));
        frame.render_widget(empty, chunks[1]);
        return;
    }

    let panel_chunks = arrange_panel_chunks(chunks[1], panels.len());
    for (chunk, panel) in panel_chunks.into_iter().zip(panels.iter()) {
        let widget = panel_widget(panel);
        frame.render_widget(widget, chunk);
    }
}

fn header_widget(
    metrics_url: &str,
    last_updated: Option<Instant>,
    last_error: Option<&str>,
    now: Instant,
    view: DashboardView,
) -> Paragraph<'static> {
    let status_style = if last_error.is_some() {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    };
    let status_text = last_error.unwrap_or("healthy");
    let updated_line = describe_last_updated(last_updated, now);

    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                "tre dashboard",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(status_text.to_string(), status_style),
        ]),
        Line::from(format!("source: {metrics_url}")),
        Line::from(updated_line),
    ];
    lines.push(view_tabs_line(view));

    Paragraph::new(lines).block(
        Block::default()
            .title("Status")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    )
}

fn view_tabs_line(view: DashboardView) -> Line<'static> {
    let mut spans = Vec::new();
    for (idx, candidate) in DashboardView::ALL.iter().enumerate() {
        let label = format!("{}·{}", idx + 1, candidate.label());
        let style = if *candidate == view {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        spans.push(Span::styled(format!("[{label}]"), style));
        if idx + 1 != DashboardView::ALL.len() {
            spans.push(Span::raw("  "));
        }
    }
    spans.push(Span::raw("   Tab/Shift+Tab  or  1-4"));
    Line::from(spans)
}

fn panel_widget(panel: &Panel) -> Paragraph<'static> {
    let mut lines = Vec::new();
    for entry in &panel.rows {
        match entry {
            PanelEntry::Stat {
                label,
                value,
                spark,
                label_style,
                value_style,
            } => {
                let mut spans = Vec::new();
                spans.push(Span::styled(format!("{:<18}", label), label_style.clone()));
                spans.push(Span::styled(format!("{:>12}", value), value_style.clone()));
                if let Some(spark) = spark {
                    spans.push(Span::raw("  "));
                    spans.push(Span::styled(
                        spark.clone(),
                        Style::default().fg(Color::LightBlue),
                    ));
                }
                lines.push(Line::from(spans));
            }
        }
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "no data",
            Style::default().fg(Color::DarkGray),
        )));
    }

    Paragraph::new(lines).block(
        Block::default()
            .title(panel.title.clone())
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    )
}

fn arrange_panel_chunks(area: Rect, panel_count: usize) -> Vec<Rect> {
    if panel_count == 0 {
        return Vec::new();
    }
    let columns = panel_count.min(MAX_PANEL_COLUMNS as usize) as u16;
    let rows = ((panel_count as u16) + columns - 1) / columns;

    let column_constraints: Vec<_> = (0..columns)
        .map(|_| Constraint::Ratio(1, columns as u32))
        .collect();
    let row_constraints: Vec<_> = (0..rows)
        .map(|_| Constraint::Ratio(1, rows as u32))
        .collect();

    let mut chunks = Vec::with_capacity(panel_count);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints(row_constraints)
        .split(area);
    for row_chunk in vertical.iter().copied() {
        let horizontal = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(column_constraints.clone())
            .split(row_chunk);
        for column_chunk in horizontal.iter().copied() {
            chunks.push(column_chunk);
            if chunks.len() == panel_count {
                return chunks;
            }
        }
    }
    chunks
}

fn wait_for_next_tick(
    started: Instant,
    refresh: Duration,
    state: &mut DashboardState,
) -> Result<bool> {
    let poll_window = Duration::from_millis(INPUT_POLL_MS.max(1));
    loop {
        let elapsed = Instant::now().saturating_duration_since(started);
        if elapsed >= refresh {
            return Ok(false);
        }
        let remaining = refresh - elapsed;
        let timeout = remaining.min(poll_window);
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    if key.code == KeyCode::Esc
                        || key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        return Ok(true);
                    }
                    match key.code {
                        KeyCode::Tab | KeyCode::Right => state.next_view(),
                        KeyCode::BackTab | KeyCode::Left => state.previous_view(),
                        KeyCode::Char('1') => state.set_view(DashboardView::Bootstrap),
                        KeyCode::Char('2') => state.set_view(DashboardView::Bundles),
                        _ => {}
                    }
                }
            }
        }
    }
}

fn describe_last_updated(last: Option<Instant>, now: Instant) -> String {
    match last {
        Some(ts) => {
            let delta = now.saturating_duration_since(ts);
            format!("updated {} ago", human_duration(delta))
        }
        None => "waiting for metrics...".to_string(),
    }
}

fn human_duration(delta: Duration) -> String {
    if delta.as_secs() >= 3600 {
        format!("{:.1}h", delta.as_secs_f64() / 3600.0)
    } else if delta.as_secs() >= 60 {
        format!("{:.1}m", delta.as_secs_f64() / 60.0)
    } else if delta.as_secs() >= 1 {
        format!("{:.1}s", delta.as_secs_f64())
    } else {
        format!("{}ms", delta.as_millis())
    }
}

fn build_planner_panel(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
) -> Option<Panel> {
    let mut panel = Panel::new("Planner");
    if let Some(rate) = counter_rate(metrics, state, now, "bundle_execution_attempts_total") {
        panel.push("Stage3 attempts/min", format_rate_per_min(rate), None);
    }
    if let Some(rate) = counter_rate(metrics, state, now, "bundle_execution_retry_events_total") {
        panel.push("Stage3 retries/min", format_rate_per_min(rate), None);
    }
    if let Some(multiplier) = gauge_value(metrics, "bundle_execution_tip_multiplier_milli", &[]) {
        panel.push(
            "Tip multiplier",
            format!("{:.3}x", multiplier / 1_000.0),
            None,
        );
    }
    if let Some(tip) = gauge_value(metrics, "bundle_execution_tip_lamports", &[]) {
        panel.push("Latest tip", format_sol_from_lamports(tip), None);
    }

    if panel.is_empty() {
        None
    } else {
        Some(panel)
    }
}

fn build_bundle_panels(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
) -> Vec<Panel> {
    let mut panels = Vec::new();
    if let Some(panel) = build_bundle_pipeline_panel(metrics, state, now) {
        panels.push(panel);
    }
    if let Some(panel) = build_bundle_history_panel(state, now) {
        panels.push(panel);
    }
    panels
}

fn build_bundle_pipeline_panel(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
) -> Option<Panel> {
    let mut panel = Panel::new("Bundle Pipeline");
    let success = counter_rate_for_label(
        metrics,
        state,
        now,
        "bundle_execution_attempts_total",
        "status",
        "success",
    );
    let failure = counter_rate_for_label(
        metrics,
        state,
        now,
        "bundle_execution_attempts_total",
        "status",
        "failure",
    );
    if success.is_some() || failure.is_some() {
        let success = success.unwrap_or(0.0);
        let failure = failure.unwrap_or(0.0);
        let total = success + failure;
        let spark = state.record_history("planner.bundle_attempts", total);
        panel.push("Bundle attempts", format_rate_per_min(total), spark);
        if total > 0.0 {
            let pct = (success / total) * 100.0;
            panel.push(
                "Stage3 success",
                format!("{:.1}%", pct),
                state.record_history("planner.bundle_success_pct", pct),
            );
        }
    }

    let accepted = counter_rate_for_label(
        metrics,
        state,
        now,
        "bundle_result_events_total",
        "status",
        "accepted",
    )
    .unwrap_or(0.0);
    let rejected = counter_rate_for_label(
        metrics,
        state,
        now,
        "bundle_result_events_total",
        "status",
        "rejected",
    )
    .unwrap_or(0.0);
    let expired = counter_rate_for_label(
        metrics,
        state,
        now,
        "bundle_result_events_total",
        "status",
        "expired",
    )
    .unwrap_or(0.0);
    let total_results = accepted + rejected + expired;
    if total_results > 0.0 {
        let success_ratio = (accepted / total_results) * 100.0;
        panel.push(
            "Jito results",
            format!(
                "{} ok | {} rej | {} exp",
                format_rate_per_min(accepted),
                format_rate_per_min(rejected),
                format_rate_per_min(expired)
            ),
            None,
        );
        panel.push(
            "Acceptance rate",
            format!("{:.1}%", success_ratio),
            state.record_history("planner.acceptance_pct", success_ratio),
        );
    }

    if panel.is_empty() {
        None
    } else {
        Some(panel)
    }
}

fn build_corecast_panel(
    metrics: &MetricMap,
    state: &mut DashboardState,
    _now: Instant,
) -> Option<Panel> {
    let mut panel = Panel::new("CoreCast");
    if let Some(lag) = gauge_value(metrics, "corecast_delta_lag_ms", &[]) {
        let spark = state.record_history("corecast.delta_lag_ms", lag);
        panel.push("Delta lag", format!("{:.0} ms", lag), spark);
    }
    if let Some(pool_reconnects) = counter_value_for_label(
        metrics,
        "corecast_stream_reconnects_total",
        "stream",
        "pool",
    ) {
        panel.push("Pool reconnects", format_count(pool_reconnects), None);
    }
    if let Some(trade_reconnects) = counter_value_for_label(
        metrics,
        "corecast_stream_reconnects_total",
        "stream",
        "trade",
    ) {
        panel.push("Trade reconnects", format_count(trade_reconnects), None);
    }

    if panel.is_empty() {
        None
    } else {
        Some(panel)
    }
}

fn gauge_value(metrics: &MetricMap, metric: &str, filters: &[(&str, &str)]) -> Option<f64> {
    metrics.get(metric).and_then(|samples| {
        samples
            .iter()
            .find(|sample| filters_match(&sample.labels, filters))
            .map(|sample| sample.value)
    })
}

fn counter_rate(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
    metric: &str,
) -> Option<f64> {
    let samples = metrics.get(metric)?;
    let mut total = 0.0;
    let mut observed = false;
    for sample in samples {
        let key = metric_key(metric, &sample.labels);
        if let Some(rate) = state.counter_rate(&key, sample.value, now) {
            total += rate;
            observed = true;
        }
    }
    if observed {
        Some(total)
    } else {
        None
    }
}

fn counter_rate_for_label(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
    metric: &str,
    label: &str,
    value: &str,
) -> Option<f64> {
    let samples = metrics.get(metric)?;
    for sample in samples {
        if sample.labels.get(label).map(String::as_str) == Some(value) {
            let key = metric_key(metric, &sample.labels);
            return state.counter_rate(&key, sample.value, now);
        }
    }
    None
}

fn counter_value_for_label(
    metrics: &MetricMap,
    metric: &str,
    label: &str,
    value: &str,
) -> Option<f64> {
    let samples = metrics.get(metric)?;
    samples
        .iter()
        .find(|sample| sample.labels.get(label).map(String::as_str) == Some(value))
        .map(|sample| sample.value)
}

fn filters_match(labels: &HashMap<String, String>, filters: &[(&str, &str)]) -> bool {
    filters
        .iter()
        .all(|(key, value)| labels.get(*key).map(String::as_str) == Some(*value))
}

fn metric_key(metric: &str, labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return metric.to_string();
    }
    let mut parts: Vec<_> = labels.iter().collect();
    parts.sort_by(|a, b| a.0.cmp(b.0));
    let mut key = String::new();
    key.push_str(metric);
    key.push('|');
    for (idx, (name, value)) in parts.iter().enumerate() {
        if idx > 0 {
            key.push(',');
        }
        key.push_str(name);
        key.push('=');
        key.push_str(value);
    }
    key
}

fn build_bootstrap_panel(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
) -> Option<Panel> {
    let mut panel = Panel::new("Readiness");
    if let Some(cache) = gauge_value(metrics, "template_cache_entries", &[]) {
        let spark = state.record_history("bootstrap.templates", cache);
        panel.push("Templates cached", format_count(cache), spark);
    }
    if let Some(rate) = counter_rate(metrics, state, now, "template_build_total") {
        panel.push("Template builds/min", format_rate_per_min(rate), None);
    }
    if let Some(success) = counter_value_for_label(
        metrics,
        "pool_bootstrap_total",
        "status",
        "success",
    ) {
        panel.push("Bootstrap success", format_count(success), None);
    }
    if let Some(failure) = counter_value_for_label(
        metrics,
        "pool_bootstrap_total",
        "status",
        "failure",
    ) {
        panel.push("Bootstrap failures", format_count(failure), None);
    }
    if panel.is_empty() {
        None
    } else {
        Some(panel)
    }
}
fn build_bundle_history_panel(state: &DashboardState, now: Instant) -> Option<Panel> {
    if state.bundle_events.is_empty() {
        return None;
    }
    let mut panel = Panel::new("Bundle History");
    for event in state.bundle_events.iter().rev().take(BUNDLE_HISTORY_LIMIT) {
        let age = now.saturating_duration_since(event.timestamp);
        let label = format!("{} · {}", human_duration(age), event.kind.label());
        let id = event
            .bundle_id
            .as_deref()
            .map(|id| short_pubkey(id))
            .unwrap_or_else(|| "n/a".to_string());
        let mut value = id;
        if let Some(detail) = &event.detail {
            value.push_str(&format!(" · {detail}"));
        }
        panel.push(label, value, None);
    }
    Some(panel)
}

fn short_pubkey(value: &str) -> String {
    const HEAD: usize = 4;
    const TAIL: usize = 4;
    if value.len() <= HEAD + TAIL + 1 {
        return value.to_string();
    }
    let head = &value[..HEAD.min(value.len())];
    let tail_start = value.len().saturating_sub(TAIL);
    let tail = &value[tail_start..];
    format!("{head}…{tail}")
}

fn ascii_sparkline(values: Vec<f64>) -> String {
    const CHARS: &[u8] = b" .:-=+*#%@";
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in &values {
        if value.is_finite() {
            if *value < min {
                min = *value;
            }
            if *value > max {
                max = *value;
            }
        }
    }
    if !min.is_finite() || !max.is_finite() {
        return String::new();
    }
    let span = (max - min).max(1e-9);
    values
        .into_iter()
        .map(|value| {
            if !value.is_finite() {
                ' '
            } else {
                let idx = (((value - min) / span) * ((CHARS.len() - 1) as f64)).round() as usize;
                CHARS[idx.min(CHARS.len() - 1)] as char
            }
        })
        .collect()
}

fn format_count(value: f64) -> String {
    if value.abs() >= 1_000.0 {
        format!("{:.1}k", value / 1_000.0)
    } else {
        format!("{:.0}", value)
    }
}

fn format_rate_per_min(rate_per_sec: f64) -> String {
    format!("{:.2}/min", rate_per_sec * 60.0)
}

fn format_sol_from_lamports(lamports: f64) -> String {
    format!("{:.4} SOL", lamports / 1_000_000_000.0)
}

fn format_bundle_hash_hint(hash: i64) -> String {
    let value = hash as u64;
    format!("0x{value:016X}")
}
