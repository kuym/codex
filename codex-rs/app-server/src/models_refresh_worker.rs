use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use codex_models_manager::manager::RefreshStrategy;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::model_catalog::ModelCatalog;
use crate::outgoing_message::ConnectionId;

const MODELS_REFRESH_INTERVAL: Duration = Duration::from_secs(4 * 60 + 30);

/// Refreshes the model catalog while an initialized client is connected.
#[derive(Debug)]
pub(crate) struct ModelsRefreshWorker {
    shutdown: CancellationToken,
    active_clients: watch::Sender<HashSet<ConnectionId>>,
    _task: JoinHandle<()>,
}

impl ModelsRefreshWorker {
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    pub(crate) fn client_initialized(&self, connection_id: ConnectionId) {
        self.active_clients.send_if_modified(|clients| {
            let inserted = clients.insert(connection_id);
            if inserted && clients.len() == 1 {
                tracing::debug!("client attached; resuming background model catalog refresh");
            }
            inserted
        });
    }

    pub(crate) fn client_closed(&self, connection_id: ConnectionId) {
        self.active_clients.send_if_modified(|clients| {
            let removed = clients.remove(&connection_id);
            if removed && clients.is_empty() {
                tracing::debug!("last client detached; pausing background model catalog refresh");
            }
            removed
        });
    }
}

impl Drop for ModelsRefreshWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(crate) fn spawn(model_catalog: &Arc<ModelCatalog>) -> ModelsRefreshWorker {
    spawn_with_interval(model_catalog, MODELS_REFRESH_INTERVAL)
}

fn spawn_with_interval(
    model_catalog: &Arc<ModelCatalog>,
    refresh_interval: Duration,
) -> ModelsRefreshWorker {
    let model_catalog = Arc::downgrade(model_catalog);
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let (active_clients, mut active_clients_rx) = watch::channel(HashSet::new());
    let task = tokio::spawn(async move {
        // Keep the deadline across disconnects so brief connections share the refresh interval.
        let mut next_refresh_at: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = worker_shutdown.cancelled() => break,
                attached = wait_for_clients(&mut active_clients_rx, /*attached*/ true) => {
                    if !attached {
                        break;
                    }
                }
            }

            if let Some(deadline) = next_refresh_at {
                tokio::select! {
                    _ = worker_shutdown.cancelled() => break,
                    _ = tokio::time::sleep_until(deadline) => {}
                    detached = wait_for_clients(&mut active_clients_rx, /*attached*/ false) => {
                        if !detached {
                            break;
                        }
                        continue;
                    }
                }
            }

            let Some(model_catalog) = model_catalog.upgrade() else {
                break;
            };
            if let Err(err) = model_catalog.list_models(RefreshStrategy::Online).await {
                // Parser diagnostics can include provider credentials from the source TOML.
                tracing::warn!(error_kind = ?err.kind(), "model catalog refresh blocked by provider requirements");
            }
            drop(model_catalog);
            next_refresh_at = Some(Instant::now() + refresh_interval);
        }
    });
    ModelsRefreshWorker {
        shutdown,
        active_clients,
        _task: task,
    }
}

/// Waits for the requested attachment state or returns `false` if the sender closes first.
async fn wait_for_clients(
    active_clients: &mut watch::Receiver<HashSet<ConnectionId>>,
    attached: bool,
) -> bool {
    active_clients
        .wait_for(|clients| clients.is_empty() != attached)
        .await
        .is_ok()
}

#[cfg(test)]
#[path = "models_refresh_worker_tests.rs"]
mod tests;
