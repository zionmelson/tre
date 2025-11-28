use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use serde::Serialize;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::stages::{ArbitrageOpportunity, BundleCandidate, PoolPath};
use crate::strategy::StrategyEngine;

pub struct Notifier {
    strategy: Arc<StrategyEngine>,
    sinks: Vec<Box<dyn AlertSink + Send + Sync>>,
    environment: Option<String>,
}

impl Notifier {
    pub fn new(strategy: Arc<StrategyEngine>, config: Arc<AppConfig>) -> Self {
        let mut sinks: Vec<Box<dyn AlertSink + Send + Sync>> = vec![Box::new(LoggingSink)];
        if let Some(url) = config.alerts.webhook_url.as_deref() {
            match WebhookSink::new(url) {
                Ok(sink) => sinks.push(Box::new(sink)),
                Err(err) => warn!(error = %err, "failed to initialize webhook notifier sink"),
            }
        }

        Self {
            strategy,
            sinks,
            environment: config.alerts.environment.clone(),
        }
    }

    pub fn notify_success(&self, message: &str) -> Result<()> {
        self.dispatch(NotificationEvent::Generic {
            environment: self.environment.clone(),
            level: "info",
            message: message.to_string(),
        });
        Ok(())
    }

    pub fn notify_arbitrage(&self, opportunities: &[ArbitrageOpportunity]) -> Result<()> {
        if opportunities.is_empty() {
            return Ok(());
        }

        for opportunity in opportunities {
            let route = describe_path(&opportunity.path);
            self.dispatch(NotificationEvent::Arbitrage {
                environment: self.environment.clone(),
                gain: opportunity.gain,
                hops: opportunity.path.len(),
                route,
            });
            self.strategy.on_arbitrage_signal(opportunity);
        }
        Ok(())
    }

    pub fn notify_bundle_candidate(&self, candidate: &BundleCandidate) -> Result<()> {
        let hop_count = candidate
            .route_plan
            .as_ref()
            .map(|plan| plan.legs.len())
            .unwrap_or(1);
        let route = candidate
            .route_plan
            .as_ref()
            .map(describe_route_plan)
            .unwrap_or_else(|| vec![candidate.pool_id.clone()]);
        self.dispatch(NotificationEvent::BundleCandidate {
            environment: self.environment.clone(),
            seq: candidate.seq,
            opportunity_seq: candidate.opportunity_seq,
            edge_bps: candidate.estimated_edge_bps,
            hop_count,
            route,
        });
        Ok(())
    }

    pub fn notify_bundle_submission(&self, event: BundleSubmissionEvent) -> Result<()> {
        self.dispatch(NotificationEvent::BundleSubmission {
            environment: self.environment.clone(),
            seq: event.seq,
            status: event.status,
            bundle_id: event.bundle_id,
            endpoint: event.endpoint,
            tip_lamports: event.tip_lamports,
            detection_latency_ms: event.detection_latency.as_secs_f64() * 1_000.0,
            slot: event.slot,
            detail: event.detail,
            error: event.error,
        });
        Ok(())
    }

    fn dispatch(&self, event: NotificationEvent) {
        for sink in &self.sinks {
            if let Err(err) = sink.send(&event) {
                warn!(sink = sink.name(), error = %err, "notifier sink failed");
            }
        }
    }
}

pub struct BundleSubmissionEvent {
    pub seq: u64,
    pub status: BundleSubmissionStatus,
    pub bundle_id: Option<String>,
    pub endpoint: Option<String>,
    pub tip_lamports: Option<u64>,
    pub detection_latency: Duration,
    pub slot: Option<u64>,
    pub detail: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BundleSubmissionStatus {
    Submitted,
    Failed,
    Accepted,
    Rejected,
    Processed,
    Finalized,
    Dropped,
    AuctionWon,
    AuctionLost,
    Expired,
}

trait AlertSink {
    fn name(&self) -> &'static str;
    fn send(&self, event: &NotificationEvent) -> Result<()>;
}

struct LoggingSink;

impl AlertSink for LoggingSink {
    fn name(&self) -> &'static str {
        "logging"
    }

    fn send(&self, event: &NotificationEvent) -> Result<()> {
        match event {
            NotificationEvent::Arbitrage {
                gain, hops, route, ..
            } => {
                info!(gain, hops, %route, "profitable cycle surfaced");
            }
            NotificationEvent::BundleCandidate {
                seq,
                edge_bps,
                hop_count,
                route,
                ..
            } => {
                info!(
                    seq,
                    edge_bps,
                    hop_count,
                    route = %route.join("->"),
                    "bundle candidate emitted"
                );
            }
            NotificationEvent::BundleSubmission {
                seq,
                status,
                bundle_id,
                endpoint,
                tip_lamports,
                slot,
                detail,
                error,
                ..
            } => {
                info!(
                    seq,
                    status = ?status,
                    bundle_id,
                    endpoint,
                    tip_lamports,
                    slot,
                    detail,
                    error,
                    "bundle submission event"
                );
            }
            NotificationEvent::Generic { message, .. } => {
                info!(%message, "notifier message");
            }
        }
        Ok(())
    }
}

struct WebhookSink {
    client: Client,
    url: String,
}

impl WebhookSink {
    fn new(url: &str) -> Result<Self> {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("webhook url is empty"));
        }
        let client = Self::build_client()?;
        Ok(Self {
            client,
            url: trimmed.to_string(),
        })
    }

    fn build_client() -> Result<Client> {
        let build = || -> Result<Client> {
            Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .context("failed to build webhook client")
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(build());
            });
            rx.recv()
                .map_err(|_| anyhow!("webhook client builder thread failed"))?
        } else {
            build()
        }
    }
}

impl AlertSink for WebhookSink {
    fn name(&self) -> &'static str {
        "webhook"
    }

    fn send(&self, event: &NotificationEvent) -> Result<()> {
        let response = self
            .client
            .post(&self.url)
            .json(event)
            .send()
            .context("failed to send webhook notification")?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "webhook responded with status {}",
                response.status()
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NotificationEvent {
    Arbitrage {
        environment: Option<String>,
        gain: f64,
        hops: usize,
        route: String,
    },
    BundleCandidate {
        environment: Option<String>,
        seq: u64,
        opportunity_seq: u64,
        edge_bps: f64,
        hop_count: usize,
        route: Vec<String>,
    },
    BundleSubmission {
        environment: Option<String>,
        seq: u64,
        status: BundleSubmissionStatus,
        bundle_id: Option<String>,
        endpoint: Option<String>,
        tip_lamports: Option<u64>,
        detection_latency_ms: f64,
        slot: Option<u64>,
        detail: Option<String>,
        error: Option<String>,
    },
    Generic {
        environment: Option<String>,
        level: &'static str,
        message: String,
    },
}

fn describe_path(path: &PoolPath) -> String {
    path.legs
        .iter()
        .map(|leg| format!("{}->{}", leg.input_mint, leg.output_mint))
        .collect::<Vec<_>>()
        .join(",")
}

fn describe_route_plan(plan: &crate::stages::types::RoutePlan) -> Vec<String> {
    plan.legs
        .iter()
        .map(|leg| format!("{}->{}", leg.input_mint, leg.output_mint))
        .collect()
}
