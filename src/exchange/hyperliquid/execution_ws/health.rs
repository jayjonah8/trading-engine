#![allow(dead_code)]

use tokio::sync::watch;


// ============================================================
// EXECUTION WS HEALTH
// ============================================================

/// Combined health state for Hyperliquid execution WebSockets.
///
/// Hyperliquid execution state is currently observed through two independent
/// user streams:
///
/// - OrderUpdates
///     -> order lifecycle
///
/// - UserEvents
///     -> actual individual executions / fills
///
/// The execution adapter should consider its execution WebSocket layer ready
/// only when BOTH streams are connected and subscribed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionWsHealth {
    pub order_updates_ready: bool,
    pub user_events_ready: bool,
}

impl ExecutionWsHealth {
    /// Returns true only when all execution-critical WebSocket streams are up.
    pub const fn is_ready(&self) -> bool {
        self.order_updates_ready
            && self.user_events_ready
    }
}


// ============================================================
// HEALTH CHANNEL
// ============================================================

/// Creates the shared watch channel used by both WebSocket tasks.
///
/// Each connection updates only its own readiness flag.
pub fn execution_ws_health_channel(
) -> (
    watch::Sender<ExecutionWsHealth>,
    watch::Receiver<ExecutionWsHealth>,
) {
    watch::channel(
        ExecutionWsHealth::default()
    )
}


// ============================================================
// WAIT UNTIL READY
// ============================================================

/// Waits until both Hyperliquid execution WebSocket subscriptions are ready.
///
/// The adapter can call this before it begins consuming `ExecCommand`s so
/// orders cannot be submitted before the execution event streams exist.
pub async fn wait_until_execution_ws_ready(
    health_rx: &mut watch::Receiver<ExecutionWsHealth>,
) -> Result<(), watch::error::RecvError> {
    loop {
        if health_rx.borrow().is_ready() {
            return Ok(());
        }

        health_rx.changed().await?;
    }
}