#![allow(dead_code)]

use tokio::sync::watch;


// ============================================================
// EXECUTION WS HEALTH
// ============================================================

/// Health state for Alpaca's execution WebSocket.
///
/// Alpaca execution state is observed through a single `trade_updates`
/// stream containing order lifecycle and execution/fill updates.
///
/// The execution adapter should consider its execution WebSocket layer ready
/// only after the connection has authenticated and Alpaca has confirmed the
/// `trade_updates` subscription.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionWsHealth {
    pub trade_updates_ready: bool,
}

impl ExecutionWsHealth {
    pub const fn is_ready(&self) -> bool {
        self.trade_updates_ready
    }
}


// ============================================================
// HEALTH CHANNEL
// ============================================================

pub fn execution_ws_health_channel(
) -> (
    watch::Sender<ExecutionWsHealth>,
    watch::Receiver<ExecutionWsHealth>,
) {
    watch::channel(ExecutionWsHealth::default())
}


// ============================================================
// WAIT UNTIL READY
// ============================================================

/// Waits until Alpaca execution monitoring is connected, authenticated,
/// and subscribed to `trade_updates`.
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