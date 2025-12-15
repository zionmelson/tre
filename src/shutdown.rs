use anyhow::{Context, Result};

/// Signals surfaced to the supervisor loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownEvent {
    /// Termination signal (Ctrl+C, SIGINT, SIGTERM).
    Terminate,
    /// Reload signal (SIGHUP).
    Reload,
}

pub struct Shutdown {
    #[cfg(unix)]
    hup: tokio::signal::unix::Signal,
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(not(unix))]
    _unused: (),
}

impl Shutdown {
    pub fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let hup = signal(SignalKind::hangup()).context("failed to bind SIGHUP handler")?;
            let term = signal(SignalKind::terminate()).context("failed to bind SIGTERM handler")?;
            Ok(Self { hup, term })
        }

        #[cfg(not(unix))]
        {
            Ok(Self { _unused: () })
        }
    }

    /// Waits for the next shutdown-related signal.
    pub async fn wait(&mut self) -> Result<ShutdownEvent> {
        #[cfg(unix)]
        {
            use tokio::select;
            select! {
                res = tokio::signal::ctrl_c() => {
                    res.context("failed to listen for ctrl-c")?;
                    Ok(ShutdownEvent::Terminate)
                }
                _ = self.term.recv() => {
                    Ok(ShutdownEvent::Terminate)
                }
                _ = self.hup.recv() => {
                    Ok(ShutdownEvent::Reload)
                }
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .context("failed to listen for ctrl-c")?;
            Ok(ShutdownEvent::Terminate)
        }
    }
}
