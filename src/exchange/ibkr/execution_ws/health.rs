#![allow(dead_code)]

use tokio::sync::watch;


// ============================================================
// IBKR EXECUTION WS HEALTH
// ============================================================

/// Health state of the persistent IBKR execution connection.
///
/// `Ready` means:
///
/// - TWS / IB Gateway connection is established
/// - the IBKR API handshake/authentication succeeded
/// - `order_update_stream()` was successfully created
/// - execution monitoring is available
///
/// New order submission and replacement should only be allowed while Ready.
///
/// Cancellation should remain available when possible because it is
/// risk-reducing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionWsHealth {
    Connecting,
    Ready,
    Disconnected,
}

impl ExecutionWsHealth {
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}


// ============================================================
// WAIT UNTIL READY
// ============================================================

/// Waits until IBKR execution monitoring becomes ready.
///
/// The runtime owns connection/reconnection attempts. This function only
/// observes health and returns once execution monitoring is available.
pub async fn wait_until_execution_ws_ready(
    health_rx: &mut watch::Receiver<ExecutionWsHealth>,
) -> Result<(), String> {
    loop {
        if health_rx.borrow().is_ready() {
            return Ok(());
        }

        if health_rx.changed().await.is_err() {
            return Err("IBKR execution WS health channel closed before becoming ready".to_string());
        }
    }
}