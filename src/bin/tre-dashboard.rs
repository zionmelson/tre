use std::cmp::Reverse;
use std::collections::HashMap;
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Frame;
use ratatui::Terminal;
use tokio::sync::{watch, Mutex};

use tre_arb_bot::config::AppConfig;
use tre_arb_bot::hydration::types::{
    CP_AMM_PROGRAM_ID, LB_CLMM_PROGRAM_ID, RAYDIUM_V3_PROGRAM_ID, WHIRLPOOL_PROGRAM_ID,
};

const DEFAULT_METRICS_URL: &str = "http://127.0.0.1:9898/metrics";
const DEFAULT_REFRESH_MS: u64 = 1_000;
const METRIC_PREFIX_HYDRATION: &str = "hydration_";
const METRIC_PREFIX_WATCHER: &str = "watcher_";
const POOL_PRICE_METRIC: &str = "hydration_pool_price_scaled";
const POOL_SLOT_METRIC: &str = "hydration_pool_slot";
const POOL_BASE_LIQUIDITY_METRIC: &str = "hydration_pool_base_liquidity";
const POOL_QUOTE_LIQUIDITY_METRIC: &str = "hydration_pool_quote_liquidity";
const POOL_PRICE_SCALE: f64 = 1_000_000.0;
const MAX_METRIC_ROWS: usize = 32;

#[derive(Clone)]
struct DashboardState {
    fetched_at: SystemTime,
    metrics_url: String,
    last_error: Option<String>,
    pools: Vec<PoolRow>,
    hydration_metrics: Vec<MetricRow>,
    watcher_metrics: Vec<MetricRow>,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            fetched_at: SystemTime::now(),
            metrics_url: DEFAULT_METRICS_URL.to_string(),
            last_error: None,
            pools: Vec::new(),
            hydration_metrics: Vec::new(),
            watcher_metrics: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ProtocolBucket {
    RaydiumV3,
    CpAmm,
    LbClmm,
    Whirlpool,
}

impl ProtocolBucket {
    fn title(&self) -> &'static str {
        match self {
            ProtocolBucket::RaydiumV3 => "Raydium v3",
            ProtocolBucket::CpAmm => "CP AMM",
            ProtocolBucket::LbClmm => "LB CLMM",
            ProtocolBucket::Whirlpool => "Whirlpool",
        }
    }

    fn from_program(program_id: &str) -> Option<Self> {
        if program_id == RAYDIUM_V3_PROGRAM_ID {
            Some(ProtocolBucket::RaydiumV3)
        } else if program_id == CP_AMM_PROGRAM_ID {
            Some(ProtocolBucket::CpAmm)
        } else if program_id == LB_CLMM_PROGRAM_ID {
            Some(ProtocolBucket::LbClmm)
        } else if program_id == WHIRLPOOL_PROGRAM_ID {
            Some(ProtocolBucket::Whirlpool)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DashboardPage {
    Metrics,
    Pools,
}

impl DashboardPage {
    fn label(&self) -> &'static str {
        match self {
            DashboardPage::Metrics => "Metrics",
            DashboardPage::Pools => "Pools",
        }
    }

    fn toggle(&self) -> Self {
        match self {
            DashboardPage::Metrics => DashboardPage::Pools,
            DashboardPage::Pools => DashboardPage::Metrics,
        }
    }
}

#[derive(Clone)]
struct DashboardStore {
    inner: Arc<Mutex<DashboardState>>,
    tx: watch::Sender<DashboardState>,
}

impl DashboardStore {
    fn new(tx: watch::Sender<DashboardState>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(DashboardState::default())),
            tx,
        }
    }

    async fn update<F>(&self, mutator: F)
    where
        F: FnOnce(&mut DashboardState),
    {
        let mut guard = self.inner.lock().await;
        mutator(&mut guard);
        let _ = self.tx.send(guard.clone());
    }
}

#[derive(Clone)]
struct SymbolResolver {
    by_mint: HashMap<String, String>,
}

impl SymbolResolver {
    fn from_config(config: &AppConfig) -> Self {
        let mut by_mint = HashMap::new();
        for (symbol, mint) in &config.tokens {
            by_mint.insert(mint.clone(), symbol.clone());
        }
        Self { by_mint }
    }

    fn label(&self, mint: &str) -> String {
        self.by_mint
            .get(mint)
            .cloned()
            .unwrap_or_else(|| truncate_pubkey(mint))
    }

    fn pair_label(&self, base_mint: &str, quote_mint: &str) -> String {
        format!("{}/{}", self.label(base_mint), self.label(quote_mint))
    }
}

impl Default for SymbolResolver {
    fn default() -> Self {
        Self {
            by_mint: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct MetricRow {
    metric: String,
    labels: Vec<(String, String)>,
    value: f64,
}

impl MetricRow {
    fn from_sample(sample: &MetricSample) -> Self {
        let mut labels: Vec<(String, String)> = sample
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            metric: sample.name.clone(),
            labels,
            value: sample.value,
        }
    }

    fn label_summary(&self) -> String {
        if self.labels.is_empty() {
            return "-".to_string();
        }
        self.labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricCategory {
    Hydration,
    Watcher,
}

#[derive(Clone)]
struct DashboardConfig {
    metrics_url: String,
    refresh_interval: Duration,
    config_path: PathBuf,
}

impl DashboardConfig {
    fn from_env() -> Self {
        let metrics_url =
            env::var("TRE_METRICS_URL").unwrap_or_else(|_| DEFAULT_METRICS_URL.to_string());
        let refresh_interval = env::var("TRE_DASHBOARD_REFRESH_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(DEFAULT_REFRESH_MS));
        let config_path = resolve_config_path();
        Self {
            metrics_url,
            refresh_interval,
            config_path,
        }
    }
}

fn resolve_config_path() -> PathBuf {
    env::var("ARBITRAGE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config.json"))
}
#[tokio::main]
async fn main() -> Result<()> {
    let config = DashboardConfig::from_env();
    let (tx, rx) = watch::channel(DashboardState::default());
    let store = DashboardStore::new(tx);
    store
        .update(|state| state.metrics_url = config.metrics_url.clone())
        .await;

    tokio::spawn(run_metrics_scraper(config.clone(), store.clone()));

    let mut resolver = Arc::new(SymbolResolver::default());
    match AppConfig::load(&config.config_path) {
        Ok(app_config) => {
            let app_config = Arc::new(app_config);
            resolver = Arc::new(SymbolResolver::from_config(&app_config));
        }
        Err(err) => {
            eprintln!(
                "failed to load {} for token labels: {err}",
                config.config_path.display()
            );
        }
    }

    let tui_resolver = resolver.clone();
    tokio::task::spawn_blocking(move || run_tui(rx, tui_resolver))
        .await
        .context("dashboard task panicked")??;

    Ok(())
}

async fn run_metrics_scraper(config: DashboardConfig, store: DashboardStore) {
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .build()
        .expect("client");
    loop {
        match fetch_snapshot(&client, &config.metrics_url).await {
            Ok(snapshot) => {
                let metrics_url = config.metrics_url.clone();
                store
                    .update(move |state| {
                        state.fetched_at = SystemTime::now();
                        state.pools = snapshot.pools;
                        state.hydration_metrics = snapshot.hydration_metrics;
                        state.watcher_metrics = snapshot.watcher_metrics;
                        state.metrics_url = metrics_url;
                        state.last_error = None;
                    })
                    .await;
            }
            Err(err) => {
                let message = err.to_string();
                store
                    .update(move |state| state.last_error = Some(message))
                    .await;
            }
        }
        tokio::time::sleep(config.refresh_interval).await;
    }
}

struct MetricsSnapshot {
    hydration_metrics: Vec<MetricRow>,
    watcher_metrics: Vec<MetricRow>,
    pools: Vec<PoolRow>,
}

async fn fetch_snapshot(client: &reqwest::Client, url: &str) -> Result<MetricsSnapshot> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to reach metrics endpoint {url}"))?
        .error_for_status()
        .with_context(|| format!("metrics endpoint returned error for {url}"))?;
    let body = response
        .text()
        .await
        .context("failed to read metrics body")?;
    Ok(parse_metrics(&body))
}

fn run_tui(rx: watch::Receiver<DashboardState>, resolver: Arc<SymbolResolver>) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let tick_rate = Duration::from_millis(200);
    let mut page = DashboardPage::Metrics;
    loop {
        let state = rx.borrow().clone();
        terminal.draw(|f| draw_ui(f, &state, page, resolver.as_ref()))?;
        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Char('1') => page = DashboardPage::Metrics,
                    KeyCode::Char('2') => page = DashboardPage::Pools,
                    KeyCode::Tab | KeyCode::BackTab => page = page.toggle(),
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct PoolKey {
    program: String,
    pool: String,
    base_mint: String,
    quote_mint: String,
}

#[derive(Clone)]
struct PoolRow {
    program_id: String,
    pool: String,
    base_mint: String,
    quote_mint: String,
    slot: u64,
    price: f64,
    base_liquidity: f64,
    quote_liquidity: f64,
}

#[derive(Default)]
struct MetricSample {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
}

fn parse_metrics(body: &str) -> MetricsSnapshot {
    let mut pool_prices: HashMap<PoolKey, f64> = HashMap::new();
    let mut pool_slots: HashMap<PoolKey, u64> = HashMap::new();
    let mut pool_base_liquidity: HashMap<PoolKey, f64> = HashMap::new();
    let mut pool_quote_liquidity: HashMap<PoolKey, f64> = HashMap::new();
    let mut hydration_metrics = Vec::new();
    let mut watcher_metrics = Vec::new();

    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(sample) = parse_sample(trimmed) {
            match sample.name.as_str() {
                POOL_PRICE_METRIC => {
                    if let Some(key) = pool_key_from_labels(&sample.labels) {
                        pool_prices.insert(key, sample.value);
                    }
                }
                POOL_SLOT_METRIC => {
                    if let Some(key) = pool_key_from_labels(&sample.labels) {
                        pool_slots.insert(key, sample.value as u64);
                    }
                }
                POOL_BASE_LIQUIDITY_METRIC => {
                    if let Some(key) = pool_key_from_labels(&sample.labels) {
                        pool_base_liquidity.insert(key, sample.value);
                    }
                }
                POOL_QUOTE_LIQUIDITY_METRIC => {
                    if let Some(key) = pool_key_from_labels(&sample.labels) {
                        pool_quote_liquidity.insert(key, sample.value);
                    }
                }
                _ => match categorize_metric(sample.name.as_str()) {
                    Some(MetricCategory::Hydration) => {
                        hydration_metrics.push(MetricRow::from_sample(&sample));
                    }
                    Some(MetricCategory::Watcher) => {
                        watcher_metrics.push(MetricRow::from_sample(&sample));
                    }
                    None => {}
                },
            }
        }
    }

    sort_metric_rows(&mut hydration_metrics);
    sort_metric_rows(&mut watcher_metrics);

    let pools = parse_pool_rows(
        pool_prices,
        pool_slots,
        pool_base_liquidity,
        pool_quote_liquidity,
    );

    MetricsSnapshot {
        hydration_metrics,
        watcher_metrics,
        pools,
    }
}

fn pool_key_from_labels(labels: &HashMap<String, String>) -> Option<PoolKey> {
    Some(PoolKey {
        program: labels.get("program")?.clone(),
        pool: labels.get("pool")?.clone(),
        base_mint: labels.get("base_mint")?.clone(),
        quote_mint: labels.get("quote_mint")?.clone(),
    })
}

fn parse_pool_rows(
    pool_prices: HashMap<PoolKey, f64>,
    pool_slots: HashMap<PoolKey, u64>,
    pool_base_liquidity: HashMap<PoolKey, f64>,
    pool_quote_liquidity: HashMap<PoolKey, f64>,
) -> Vec<PoolRow> {
    let mut rows = Vec::with_capacity(pool_prices.len());
    for (key, price_scaled) in pool_prices {
        let slot = pool_slots.get(&key).copied().unwrap_or_default();
        let base_liquidity = pool_base_liquidity.get(&key).copied().unwrap_or_default();
        let quote_liquidity = pool_quote_liquidity.get(&key).copied().unwrap_or_default();
        rows.push(PoolRow {
            program_id: key.program,
            pool: key.pool,
            base_mint: key.base_mint,
            quote_mint: key.quote_mint,
            slot,
            price: price_scaled / POOL_PRICE_SCALE,
            base_liquidity,
            quote_liquidity,
        });
    }
    rows.sort_by_key(|row| Reverse(row.slot));
    rows
}

fn parse_sample(line: &str) -> Option<MetricSample> {
    let mut parts = line.split_whitespace();
    let head = parts.next()?;
    let value_str = parts.next()?;
    let value = value_str.parse::<f64>().ok()?;

    if let Some(start) = head.find('{') {
        let end = head.rfind('}')?;
        let name = head[..start].to_string();
        let labels_str = &head[start + 1..end];
        let labels = parse_metric_labels(labels_str);
        Some(MetricSample {
            name,
            labels,
            value,
        })
    } else {
        Some(MetricSample {
            name: head.to_string(),
            labels: HashMap::new(),
            value,
        })
    }
}

fn parse_metric_labels(segment: &str) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    for entry in segment.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(eq) = trimmed.find('=') {
            let key = trimmed[..eq].trim();
            let value = trimmed[eq + 1..].trim().trim_matches('"');
            labels.insert(key.to_string(), value.to_string());
        }
    }
    labels
}

fn categorize_metric(name: &str) -> Option<MetricCategory> {
    if name.starts_with(METRIC_PREFIX_HYDRATION) {
        Some(MetricCategory::Hydration)
    } else if name.starts_with(METRIC_PREFIX_WATCHER) {
        Some(MetricCategory::Watcher)
    } else {
        None
    }
}

fn sort_metric_rows(rows: &mut Vec<MetricRow>) {
    rows.sort_by(|a, b| a.metric.cmp(&b.metric).then(a.labels.cmp(&b.labels)));
    if rows.len() > MAX_METRIC_ROWS {
        rows.truncate(MAX_METRIC_ROWS);
    }
}
fn draw_ui(f: &mut Frame, state: &DashboardState, page: DashboardPage, resolver: &SymbolResolver) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
        .split(f.size());
    draw_tabs(f, chunks[0], page);

    match page {
        DashboardPage::Metrics => draw_metrics_page(f, chunks[1], state),
        DashboardPage::Pools => draw_pools_page(f, chunks[1], state, resolver),
    }
}

fn draw_metrics_page(f: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .title(format!(
            "Hydration & Watcher metrics | endpoint {}",
            state.metrics_url
        ))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if let Some(error) = &state.last_error {
        let paragraph = Paragraph::new(error.clone())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::NONE));
        f.render_widget(paragraph, inner);
        return;
    }

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
        .split(inner);

    render_metric_group(f, columns[0], "Hydration metrics", &state.hydration_metrics);
    render_metric_group(f, columns[1], "Watcher metrics", &state.watcher_metrics);
}

fn draw_pools_page(f: &mut Frame, area: Rect, state: &DashboardState, resolver: &SymbolResolver) {
    let block = Block::default()
        .title("Pools by protocol (from Prometheus)")
        .borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if let Some(error) = &state.last_error {
        let paragraph = Paragraph::new(error.clone())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::NONE));
        f.render_widget(paragraph, inner);
        return;
    }

    if state.pools.is_empty() {
        let paragraph = Paragraph::new("No pools observed yet.")
            .style(Style::default().fg(Color::Yellow))
            .block(Block::default().borders(Borders::NONE));
        f.render_widget(paragraph, inner);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
        .split(inner);

    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
        .split(chunks[0]);
    let bottom_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)].as_ref())
        .split(chunks[1]);

    render_protocol_tables(f, top_chunks[0], ProtocolBucket::RaydiumV3, state, resolver);
    render_protocol_tables(f, top_chunks[1], ProtocolBucket::CpAmm, state, resolver);
    render_protocol_tables(f, bottom_chunks[0], ProtocolBucket::LbClmm, state, resolver);
    render_protocol_tables(
        f,
        bottom_chunks[1],
        ProtocolBucket::Whirlpool,
        state,
        resolver,
    );
}

fn render_protocol_tables(
    f: &mut Frame,
    area: Rect,
    bucket: ProtocolBucket,
    state: &DashboardState,
    resolver: &SymbolResolver,
) {
    let rows: Vec<PoolRow> = state
        .pools
        .iter()
        .filter(|pool| ProtocolBucket::from_program(&pool.program_id) == Some(bucket))
        .cloned()
        .collect();

    let block = Block::default().title(bucket.title()).borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if rows.is_empty() {
        let paragraph = Paragraph::new("—")
            .style(Style::default().fg(Color::DarkGray))
            .block(Block::default().borders(Borders::NONE));
        f.render_widget(paragraph, inner);
        return;
    }

    let table = render_pool_table(rows, resolver);
    f.render_widget(table, inner);
}

fn render_metric_group(f: &mut Frame, area: Rect, title: &str, metrics: &[MetricRow]) {
    let block = Block::default().title(title).borders(Borders::ALL);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if metrics.is_empty() {
        let paragraph = Paragraph::new("No samples yet")
            .style(Style::default().fg(Color::Yellow))
            .block(Block::default().borders(Borders::NONE));
        f.render_widget(paragraph, inner);
        return;
    }

    let header = Row::new(vec![
        Cell::from("Metric"),
        Cell::from("Labels"),
        Cell::from("Value"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = metrics.iter().take(MAX_METRIC_ROWS).map(|row| {
        Row::new(vec![
            Cell::from(row.metric.clone()),
            Cell::from(row.label_summary()),
            Cell::from(format_metric_value(row.value)),
        ])
    });

    let widths = [
        Constraint::Percentage(40),
        Constraint::Percentage(40),
        Constraint::Percentage(20),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .column_spacing(1)
        .block(Block::default().borders(Borders::NONE));
    f.render_widget(table, inner);
}

fn render_pool_table(rows: Vec<PoolRow>, resolver: &SymbolResolver) -> Table<'static> {
    let header = Row::new(vec![
        Cell::from("Pool"),
        Cell::from("Pair"),
        Cell::from("Slot"),
        Cell::from("Price"),
        Cell::from("Base Liquidity"),
        Cell::from("Quote Liquidity"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let data_rows = rows.into_iter().map(|row| {
        let pair = resolver.pair_label(&row.base_mint, &row.quote_mint);
        Row::new(vec![
            Cell::from(truncate_pubkey(&row.pool)),
            Cell::from(pair),
            Cell::from(row.slot.to_string()),
            Cell::from(format!("{:.6}", row.price)),
            Cell::from(format_number(row.base_liquidity)),
            Cell::from(format_number(row.quote_liquidity)),
        ])
    });

    let widths = [
        Constraint::Length(12),
        Constraint::Length(16),
        Constraint::Length(12),
        Constraint::Length(14),
        Constraint::Length(16),
        Constraint::Min(10),
    ];

    Table::new(data_rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::NONE))
        .column_spacing(1)
}

fn draw_tabs(f: &mut Frame, area: Rect, page: DashboardPage) {
    let titles: Vec<Line> = [DashboardPage::Metrics, DashboardPage::Pools]
        .iter()
        .map(|tab| {
            let style = if *tab == page {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Line::from(Span::styled(tab.label(), style))
        })
        .collect();

    let block = Block::default()
        .title("tre-dashboard")
        .borders(Borders::ALL);

    let tabs = ratatui::widgets::Tabs::new(titles)
        .highlight_style(Style::default().fg(Color::Cyan))
        .select(match page {
            DashboardPage::Metrics => 0,
            DashboardPage::Pools => 1,
        })
        .block(block);

    f.render_widget(tabs, area);
}

fn format_number(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.2}K", value / 1_000.0)
    } else {
        format!("{value:.2}")
    }
}

fn format_metric_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.abs() >= 1_000.0 {
        format_number(value)
    } else if value.fract().abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        format!("{value:.3}")
    }
}

fn truncate_pubkey(key: &str) -> String {
    if key.len() <= 8 {
        key.to_string()
    } else {
        format!("{}…{}", &key[..4], &key[key.len() - 3..])
    }
}
