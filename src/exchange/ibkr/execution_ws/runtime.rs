#![allow(dead_code)]

use ibapi::{
    orders::OrderUpdate,
    subscriptions::SubscriptionItem,
};
use tokio::sync::{
    mpsc::{self, UnboundedReceiver},
    watch,
};

use crate::exchange::ibkr::execution_ws::{
    health::ExecutionWsHealth,
    order_updates::run_ibkr_order_updates,
};


// ============================================================
// IBKR EXECUTION WS
// ============================================================

/// Handle returned to the IBKR adapter.
///
/// The adapter consumes raw IBKR order updates through `raw_rx` and observes
/// execution-monitoring health through `health_rx`.
///
/// The underlying transport is the persistent IBKR TWS / IB Gateway API
/// connection rather than a literal WebSocket. The `execution_ws` module name
/// is retained as the exchange-adapter execution-monitoring abstraction.
pub struct IbkrExecutionWs {
    pub raw_rx: UnboundedReceiver<SubscriptionItem<OrderUpdate>>,
    pub health_rx: watch::Receiver<ExecutionWsHealth>,
}


// ============================================================
// START EXECUTION WS
// ============================================================

/// Starts persistent IBKR execution monitoring.
///
/// The spawned supervisor owns:
///
/// - TWS / IB Gateway connection lifecycle
/// - IBKR API handshake/authentication
/// - order-update stream creation
/// - execution-monitoring health
/// - automatic reconnection
///
/// Raw IBKR updates are forwarded to the adapter where they are mapped into
/// canonical Engine `ExecEvent`s.
pub fn start_execution_ws(
    address: String,
    client_id: i32,
    debug_mode: bool,
) -> IbkrExecutionWs {
    let (tx_raw, raw_rx) = mpsc::unbounded_channel::<SubscriptionItem<OrderUpdate>>();
    let (health_tx, health_rx) = watch::channel(ExecutionWsHealth::Connecting);

    tokio::spawn(async move {
        run_ibkr_order_updates(address, client_id, tx_raw, health_tx, debug_mode).await;
    });

    IbkrExecutionWs { raw_rx, health_rx }
}