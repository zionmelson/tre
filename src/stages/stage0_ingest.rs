use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::runtime::Builder as TokioRuntime;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

use crate::core::{RingSender, SlabPool, SlabRef};
use crate::corecast::{CorecastSubscriber, Opportunity, OpportunityError};
use crate::protocol::ProtocolRegistry;

use super::spawn_stage_thread;

pub type OpportunityLease = SlabRef<Opportunity>;

pub struct Stage0Ingest;

impl Stage0Ingest {
    pub fn spawn(
        corecast: CorecastSubscriber,
        protocols: Arc<ProtocolRegistry>,
        pool: SlabPool<Opportunity>,
        tx: RingSender<OpportunityLease>,
        running: Arc<AtomicBool>,
        core_index: Option<usize>,
    ) -> std::thread::JoinHandle<()> {
        spawn_stage_thread("stage0-ingest", core_index, move || {
            info!(target = "stage0", cap = pool.capacity(), "ingest online");
            Self::run(corecast, protocols.clone(), pool, tx, running);
        })
    }

    fn run(
        corecast: CorecastSubscriber,
        protocols: Arc<ProtocolRegistry>,
        pool: SlabPool<Opportunity>,
        tx: RingSender<OpportunityLease>,
        running: Arc<AtomicBool>,
    ) {
        let runtime = TokioRuntime::new_current_thread()
            .enable_all()
            .build()
            .expect("stage0 runtime");

        if let Err(err) =
            runtime.block_on(Self::pump(corecast, protocols, pool, tx, running.clone()))
        {
            error!(target = "stage0", error = %err, "ingest loop exited with error");
        }

        info!(target = "stage0", "ingest shutdown");
    }

    async fn pump(
        corecast: CorecastSubscriber,
        protocols: Arc<ProtocolRegistry>,
        pool: SlabPool<Opportunity>,
        tx: RingSender<OpportunityLease>,
        running: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut retry_delay = Duration::from_millis(250);
        let max_delay = Duration::from_secs(5);

        while running.load(Ordering::Acquire) {
            match corecast.subscribe_trades().await {
                Ok(mut stream) => {
                    info!(target = "stage0", "CoreCast trade stream subscribed");
                    retry_delay = Duration::from_millis(250);
                    while running.load(Ordering::Acquire) {
                        match stream.message().await {
                            Ok(Some(message)) => {
                                match Opportunity::from_trade(message, &protocols) {
                                    Ok(opportunity) => {
                                        if let Some(slot) = pool.try_insert(opportunity) {
                                            if !tx.spin_push(slot, &running) {
                                                break;
                                            }
                                        } else {
                                            warn!(
                                                target = "stage0",
                                                "opportunity slab saturated; dropping event"
                                            );
                                        }
                                    }
                                    Err(OpportunityError::UnknownProtocol(program)) => {
                                        warn!(
                                            target = "stage0",
                                            program = %program,
                                            "dropping trade for unmapped protocol"
                                        );
                                    }
                                    Err(err) => {
                                        trace!(
                                            target = "stage0",
                                            error = %err,
                                            "dropping malformed trade message"
                                        );
                                    }
                                }
                            }
                            Ok(None) => {
                                warn!(target = "stage0", "CoreCast closed trade stream");
                                break;
                            }
                            Err(status) => {
                                warn!(target = "stage0", status = %status, "CoreCast stream error");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(target = "stage0", error = %err, "failed to subscribe to CoreCast trades");
                }
            }

            if !running.load(Ordering::Acquire) {
                break;
            }

            sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(max_delay);
            info!(target = "stage0", "retrying CoreCast subscription");
        }

        Ok(())
    }
}
