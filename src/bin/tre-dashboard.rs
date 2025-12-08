use std::cmp::Ordering;
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
use tre_arb_bot::hydration::types::{
    CP_AMM_PROGRAM_ID, LB_CLMM_PROGRAM_ID, RAYDIUM_V3_PROGRAM_ID, WHIRLPOOL_PROGRAM_ID,
};

const DEFAULT_METRICS_URL: &str = "http://127.0.0.1:9898/metrics";
const DEFAULT_REFRESH_MS: u64 = 1_000;
const DEFAULT_HISTORY: usize = 60;
const PANEL_ENV: &str = "TRE_DASHBOARD_PANELS";
const HISTORY_ENV: &str = "TRE_DASHBOARD_HISTORY";
const INPUT_POLL_MS: u64 = 50;
const MAX_PANEL_COLUMNS: u16 = 3;
const INITIAL_LATENCY_ROW_LIMIT: usize = 6;
const PROTOCOL_LATENCY_POOL_ROW_LIMIT: usize = 8;
const NAMED_PROTOCOLS: &[(&str, &str)] = &[
    (RAYDIUM_V3_PROGRAM_ID, "Raydium v3"),
    (CP_AMM_PROGRAM_ID, "CP AMM"),
    (LB_CLMM_PROGRAM_ID, "LB CLMM"),
    (WHIRLPOOL_PROGRAM_ID, "Whirlpool"),
];

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
            Ok(val) => val,
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
    view: DashboardView,
}

impl DashboardState {
    fn new(history_len: usize) -> Self {
        Self {
            history: MetricHistory::new(history_len),
            counters: HashMap::new(),
            view: DashboardView::Summary,
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DashboardView {
    Summary,
    HydrationLatency,
}

impl DashboardView {
    const ALL: [DashboardView; 2] = [DashboardView::Summary, DashboardView::HydrationLatency];

    fn label(self) -> &'static str {
        match self {
            DashboardView::Summary => "Ops",
            DashboardView::HydrationLatency => "Hydration",
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
    rows: Vec<PanelRow>,
}

impl Panel {
    fn new(title: &str) -> Self {
        Self {
            title: title.to_string(),
            rows: Vec::new(),
        }
    }

    fn push(&mut self, label: impl Into<String>, value: impl Into<String>, spark: Option<String>) {
        self.rows.push(PanelRow {
            label: label.into(),
            value: value.into(),
            spark,
        });
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

struct PanelRow {
    label: String,
    value: String,
    spark: Option<String>,
}

fn build_panels(
    metrics: &MetricMap,
    kinds: &[PanelKind],
    state: &mut DashboardState,
    now: Instant,
) -> Vec<Panel> {
    match state.view() {
        DashboardView::Summary => build_summary_panels(metrics, kinds, state, now),
        DashboardView::HydrationLatency => build_protocol_latency_panels(metrics),
    }
}

fn build_summary_panels(
    metrics: &MetricMap,
    kinds: &[PanelKind],
    state: &mut DashboardState,
    now: Instant,
) -> Vec<Panel> {
    let mut output = Vec::new();
    for kind in kinds {
        match kind {
            PanelKind::Readiness => {
                if let Some(panel) = build_readiness_panel(metrics, state) {
                    output.push(panel);
                }
            }
            PanelKind::Planner => {
                if let Some(panel) = build_planner_panel(metrics, state, now) {
                    output.push(panel);
                }
            }
            PanelKind::Rpc => {
                if let Some(panel) = build_rpc_panel(metrics, state, now) {
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
    spans.push(Span::raw("   Tab/Shift+Tab  or  1/2"));
    Line::from(spans)
}

fn panel_widget(panel: &Panel) -> Paragraph<'static> {
    let mut lines = Vec::new();
    for row in &panel.rows {
        let mut spans = Vec::new();
        spans.push(Span::styled(
            format!("{:<18}", row.label),
            Style::default().fg(Color::Gray),
        ));
        spans.push(Span::styled(
            format!("{:>12}", row.value),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        if let Some(spark) = &row.spark {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                spark.clone(),
                Style::default().fg(Color::LightBlue),
            ));
        }
        lines.push(Line::from(spans));
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
                        KeyCode::Char('1') => state.set_view(DashboardView::Summary),
                        KeyCode::Char('2') => state.set_view(DashboardView::HydrationLatency),
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

fn build_readiness_panel(metrics: &MetricMap, state: &mut DashboardState) -> Option<Panel> {
    let mut panel = Panel::new("Readiness");
    if let Some(ready) = gauge_value(metrics, "hydration_ready_pools", &[]) {
        let spark = state.record_history("readiness.ready", ready);
        panel.push("Ready pools", format_count(ready), spark);
    }
    if let (Some(tracked), Some(ready)) = (
        gauge_value(metrics, "hydration_tracked_pools", &[]),
        gauge_value(metrics, "hydration_ready_pools", &[]),
    ) {
        let warming = (tracked - ready).max(0.0);
        let spark = state.record_history("readiness.warming", warming);
        panel.push("Warming pools", format_count(warming), spark);
    }
    if let Some(entries) = gauge_value(metrics, "hydration_template_cache_entries", &[]) {
        panel.push("Template cache entries", format_count(entries), None);
    }
    if let Some(age) = gauge_value(metrics, "hydration_template_age_ms", &[]) {
        let spark = state.record_history("readiness.template_age", age);
        panel.push("Template age", format_duration_ms(age), spark);
    }
    if let Some(items) = gauge_value(metrics, "hydration_queue_depth_items", &[]) {
        let pct = gauge_value(metrics, "hydration_queue_depth_pct", &[]).unwrap_or(0.0);
        let spark = state.record_history("readiness.queue_items", items);
        panel.push(
            "Hydrator queue",
            format!("{} items ({:.0}%)", format_count(items), pct),
            spark,
        );
    }
    if let Some(summary) = component_lag_summary(metrics, "hydration_component_lag_ms", "component")
    {
        panel.push("Hydration lag (ms)", summary, None);
    }
    if let Some((total, entries)) = initial_latency_entries(metrics, INITIAL_LATENCY_ROW_LIMIT) {
        panel.push(
            "Initial hydration",
            format!("{} pools", format_count(total as f64)),
            None,
        );
        for (pool_label, latency_ms) in entries {
            panel.push(
                format!("↳ {}", pool_label),
                format_duration_ms(latency_ms),
                None,
            );
        }
    }

    if panel.is_empty() {
        None
    } else {
        Some(panel)
    }
}

fn build_planner_panel(
    metrics: &MetricMap,
    state: &mut DashboardState,
    now: Instant,
) -> Option<Panel> {
    let mut panel = Panel::new("Planner / Submission");
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

    if let Some(depth) = gauge_value(metrics, "fastlane_ring_depth_items", &[]) {
        let spark = state.record_history("planner.ring_depth", depth);
        let pct = gauge_value(metrics, "fastlane_ring_depth_pct", &[]).unwrap_or(0.0);
        panel.push(
            "Ring depth",
            format!("{} items ({:.0}%)", format_count(depth), pct),
            spark,
        );
    }
    if let Some(drop_rate) = counter_rate(metrics, state, now, "fastlane_ring_drop_events_total") {
        panel.push("Ring drops", format_rate_per_min(drop_rate), None);
    }
    if let Some(free_pct) = gauge_value(metrics, "fastlane_slab_free_pct", &[]) {
        let free_slots = gauge_value(metrics, "fastlane_slab_free_slots", &[]).unwrap_or(0.0);
        panel.push(
            "Slab capacity",
            format!("{:.0}% free ({} slots)", free_pct, format_count(free_slots)),
            None,
        );
    }

    if panel.is_empty() {
        None
    } else {
        Some(panel)
    }
}

fn build_rpc_panel(metrics: &MetricMap, state: &mut DashboardState, now: Instant) -> Option<Panel> {
    let mut panel = Panel::new("RPC / CoreCast");
    if let Some(rate) = counter_rate(metrics, state, now, "hydration_adapter_ingest_total") {
        panel.push("CoreCast ingest", format_rate_per_min(rate), None);
    }
    if let Some(rate) = counter_rate(metrics, state, now, "hydration_rpc_refresh_failure_total") {
        panel.push("RPC refresh failures", format_rate_per_min(rate), None);
    }
    if let Some(rate) = counter_rate(metrics, state, now, "hydration_rpc_refresh_total") {
        panel.push("RPC refresh", format_rate_per_min(rate), None);
    }
    if let Some(rate) = counter_rate(metrics, state, now, "hydration_job_queue_failures_total") {
        panel.push("Queue failures", format_rate_per_min(rate), None);
    }
    if let Some(backlog) = gauge_value(metrics, "watcher_update_backlog", &[]) {
        let spark = state.record_history("rpc.backlog", backlog);
        panel.push("Watcher backlog", format_count(backlog), spark);
    }
    if let Some(summary) =
        component_lag_summary(metrics, "hydration_component_slot_delta", "component")
    {
        panel.push("Slot lag (slots)", summary, None);
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

fn component_lag_summary(metrics: &MetricMap, metric: &str, label_key: &str) -> Option<String> {
    let samples = metrics.get(metric)?;
    let mut aggregates: HashMap<String, f64> = HashMap::new();
    for sample in samples {
        if let Some(label) = sample.labels.get(label_key) {
            if sample.value.is_finite() {
                aggregates
                    .entry(label.clone())
                    .and_modify(|v| *v = v.max(sample.value))
                    .or_insert(sample.value);
            }
        }
    }
    if aggregates.is_empty() {
        return None;
    }
    let mut entries: Vec<_> = aggregates.into_iter().collect();
    entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    let summary = entries
        .into_iter()
        .take(3)
        .map(|(label, value)| format!("{}:{:.0}", label, value))
        .collect::<Vec<_>>()
        .join(" ");
    Some(summary)
}

fn initial_latency_entries(
    metrics: &MetricMap,
    limit: usize,
) -> Option<(usize, Vec<(String, f64)>)> {
    if limit == 0 {
        return None;
    }
    let samples = metrics.get("hydration_pool_initial_latency_ms")?;
    let mut per_pool: HashMap<String, f64> = HashMap::new();
    for sample in samples {
        if !sample.value.is_finite() {
            continue;
        }
        if let Some(pool) = sample.labels.get("pool") {
            per_pool.insert(pool.clone(), sample.value);
        }
    }
    if per_pool.is_empty() {
        return None;
    }
    let total = per_pool.len();
    let mut entries: Vec<_> = per_pool.into_iter().collect();
    entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    entries.truncate(limit);
    let rows = entries
        .into_iter()
        .map(|(pool, latency)| (short_pubkey(&pool), latency))
        .collect();
    Some((total, rows))
}

fn build_protocol_latency_panels(metrics: &MetricMap) -> Vec<Panel> {
    let mut buckets = collect_protocol_latency(metrics);
    let mut panels = Vec::new();

    for (program, label) in NAMED_PROTOCOLS {
        let entries = buckets.remove(*program).unwrap_or_default();
        panels.push(protocol_latency_panel(label, entries));
    }

    if !buckets.is_empty() {
        let mut combined = HashMap::new();
        for (_, entries) in buckets.drain() {
            combined.extend(entries);
        }
        panels.push(protocol_latency_panel("Other", combined));
    }

    if panels.is_empty() {
        panels.push(protocol_latency_panel("Hydration", HashMap::new()));
    }

    panels
}

fn collect_protocol_latency(metrics: &MetricMap) -> HashMap<String, HashMap<String, f64>> {
    let mut buckets: HashMap<String, HashMap<String, f64>> = HashMap::new();
    let Some(samples) = metrics.get("hydration_pool_initial_latency_ms") else {
        return buckets;
    };
    for sample in samples {
        let Some(program) = sample.labels.get("program").cloned() else {
            continue;
        };
        let Some(pool) = sample.labels.get("pool").cloned() else {
            continue;
        };
        let latency = sample.value;
        if !latency.is_finite() || latency < 0.0 {
            continue;
        }
        buckets
            .entry(program)
            .or_insert_with(HashMap::new)
            .insert(pool, latency);
    }
    buckets
}

fn protocol_latency_panel(label: &str, entries: HashMap<String, f64>) -> Panel {
    let mut panel = Panel::new(&format!("Hydration Latency · {label}"));
    let pool_count = entries.len();
    panel.push("Pools observed", format_count(pool_count as f64), None);
    if pool_count == 0 {
        return panel;
    }

    let mut latencies: Vec<f64> = entries.values().copied().collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let median_ms = percentile_from_sorted(&latencies, 0.5);
    let p95_ms = percentile_from_sorted(&latencies, 0.95);
    let mean_ms = latencies.iter().copied().sum::<f64>() / pool_count as f64;
    panel.push(
        "Median | p95",
        format!(
            "{} | {}",
            format_duration_ms(median_ms),
            format_duration_ms(p95_ms)
        ),
        None,
    );
    panel.push("Average", format_duration_ms(mean_ms), None);

    let mut ranked: Vec<_> = entries.into_iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    for (idx, (pool, latency)) in ranked
        .into_iter()
        .take(PROTOCOL_LATENCY_POOL_ROW_LIMIT)
        .enumerate()
    {
        panel.push(
            format!("{:>2}. {}", idx + 1, short_pubkey(&pool)),
            format_duration_ms(latency),
            None,
        );
    }
    if pool_count > PROTOCOL_LATENCY_POOL_ROW_LIMIT {
        panel.push(
            "More pools",
            format_count((pool_count - PROTOCOL_LATENCY_POOL_ROW_LIMIT) as f64),
            None,
        );
    }
    panel
}

fn percentile_from_sorted(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    if values.len() == 1 {
        return values[0];
    }
    let clamped = quantile.clamp(0.0, 1.0);
    let position = clamped * ((values.len() - 1) as f64);
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        return values[lower];
    }
    let weight = position - lower as f64;
    values[lower] * (1.0 - weight) + values[upper] * weight
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

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1_000.0 {
        format!("{:.2}s", ms / 1_000.0)
    } else {
        format!("{:.0}ms", ms)
    }
}
