use std::collections::BTreeMap;
use std::env;
use std::io;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Frame;
use ratatui::Terminal;
use tokio::sync::watch;

const DEFAULT_METRICS_URL: &str = "http://127.0.0.1:9898/metrics";
const DEFAULT_REFRESH_MS: u64 = 1_000;
const METRIC_NAME: &str = "hydration_adapter_ingest_total_total";

#[derive(Clone)]
struct DashboardState {
    fetched_at: SystemTime,
    adapters: Vec<AdapterStat>,
    metrics_url: String,
    last_error: Option<String>,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            fetched_at: SystemTime::now(),
            adapters: Vec::new(),
            metrics_url: DEFAULT_METRICS_URL.to_string(),
            last_error: None,
        }
    }
}

#[derive(Clone, Default)]
struct AdapterStat {
    name: String,
    total: f64,
}

#[derive(Clone)]
struct DashboardConfig {
    metrics_url: String,
    refresh_interval: Duration,
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
        Self {
            metrics_url,
            refresh_interval,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = DashboardConfig::from_env();
    let (tx, rx) = watch::channel(DashboardState::default());

    tokio::spawn(run_scraper(config.clone(), tx));

    tokio::task::spawn_blocking(move || run_tui(rx))
        .await
        .context("dashboard task panicked")??;

    Ok(())
}

async fn run_scraper(config: DashboardConfig, tx: watch::Sender<DashboardState>) {
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .build()
        .expect("client");
    let mut last_good = DashboardState::default();
    loop {
        let next = match fetch_snapshot(&client, &config.metrics_url).await {
            Ok(snapshot) => {
                last_good = snapshot;
                last_good.clone()
            }
            Err(err) => {
                let mut errored = last_good.clone();
                errored.last_error = Some(err.to_string());
                errored
            }
        };
        let _ = tx.send_replace(next);
        tokio::time::sleep(config.refresh_interval).await;
    }
}

async fn fetch_snapshot(client: &reqwest::Client, url: &str) -> Result<DashboardState> {
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
    let adapters = parse_adapter_counts(&body);
    Ok(DashboardState {
        fetched_at: SystemTime::now(),
        adapters,
        metrics_url: url.to_string(),
        last_error: None,
    })
}

fn parse_adapter_counts(body: &str) -> Vec<AdapterStat> {
    let mut totals = BTreeMap::new();
    for line in body.lines() {
        if !line.starts_with(METRIC_NAME) {
            continue;
        }
        if let Some((metric, value)) = line.split_once(' ') {
            if let Ok(total) = value.trim().parse::<f64>() {
                let label_value =
                    extract_label(metric, "adapter").unwrap_or_else(|| "unknown".to_string());
                totals.insert(label_value, total);
            }
        }
    }
    totals
        .into_iter()
        .map(|(name, total)| AdapterStat { name, total })
        .collect()
}

fn extract_label(metric: &str, label: &str) -> Option<String> {
    let start = metric.find('{')?;
    let end = metric.find('}')?;
    let labels = &metric[start + 1..end];
    for pair in labels.split(',') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next()?.trim();
        let value = parts.next()?.trim().trim_matches('"');
        if key == label {
            return Some(value.to_string());
        }
    }
    None
}

fn run_tui(rx: watch::Receiver<DashboardState>) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let tick_rate = Duration::from_millis(200);
    loop {
        let state = rx.borrow().clone();
        terminal.draw(|f| draw_ui(f, &state))?;
        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn draw_ui(frame: &mut Frame<'_>, state: &DashboardState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.size());

    let header = Paragraph::new(vec![
        Line::from(Span::styled(
            "TRE Analytics Dashboard",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("Metrics source: {}", state.metrics_url)),
        Line::from(format!("Last update: {}", format_age(state.fetched_at))),
        Line::from("Press q to exit"),
    ])
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, layout[0]);

    let rows: Vec<Row> = if state.adapters.is_empty() {
        vec![Row::new(vec![
            Cell::from("waiting for metrics"),
            Cell::from("-"),
        ])]
    } else {
        state
            .adapters
            .iter()
            .map(|adapter| {
                Row::new(vec![
                    Cell::from(adapter.name.clone()),
                    Cell::from(format!("{:.0}", adapter.total)),
                ])
            })
            .collect()
    };

    let table = Table::new(
        rows,
        [Constraint::Percentage(70), Constraint::Percentage(30)],
    )
    .header(
        Row::new(vec![Cell::from("Protocol"), Cell::from("Total Events")])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title("CoreCast Pool Events")
            .borders(Borders::ALL),
    )
    .column_spacing(2);
    frame.render_widget(table, layout[1]);

    let footer_text = match &state.last_error {
        Some(err) => Line::from(Span::styled(err.as_str(), Style::default().fg(Color::Red))),
        None => Line::from(Span::styled(
            "Metrics streaming normally",
            Style::default().fg(Color::Green),
        )),
    };
    let footer = Paragraph::new(footer_text).block(Block::default().borders(Borders::ALL));
    frame.render_widget(footer, layout[2]);
}

fn format_age(ts: SystemTime) -> String {
    match SystemTime::now().duration_since(ts) {
        Ok(dur) => format!("{:.1}s ago", dur.as_secs_f32()),
        Err(_) => "just now".to_string(),
    }
}
