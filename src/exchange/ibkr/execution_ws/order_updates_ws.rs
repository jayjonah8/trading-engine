#![allow(dead_code)]

use futures::StreamExt;
use ibapi::{
    orders::OrderUpdate,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};
use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::{
    exchange::ibkr::execution_ws::health::ExecutionWsHealth,
    shared::print_log,
};


// ============================================================
// RUN IBKR ORDER UPDATES
// ============================================================

/// Runs the persistent IBKR order/execution update stream.
///
/// IBKR does not expose a separate execution WebSocket. Order lifecycle,
/// executions, commissions, and order notices are received through
/// `order_update_stream()` over the TWS / IB Gateway API connection.
///
/// Connection-level recovery is handled by the IBKR client layer. This task
/// owns the order-update subscription and forwards its events to the adapter.
pub(crate) async fn run_ibkr_order_updates(
    client: &Client,
    tx_raw: UnboundedSender<SubscriptionItem<OrderUpdate>>,
    health_tx: watch::Sender<ExecutionWsHealth>,
    debug_mode: bool,
) {
    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    set_execution_health(&health_tx, ExecutionWsHealth::Connecting);

    // ============================================================
    // CREATE ORDER UPDATE STREAM
    // ============================================================

    let mut order_stream = match client.order_update_stream().await {
        Ok(stream) => stream,

        Err(error) => {
            set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);
            eprintln!("[IBKR EXECUTION WS] ORDER UPDATE STREAM FAILED | error={:?}", error);
            return;
        }
    };


    // ============================================================
    // READY
    // ============================================================

    set_execution_health(&health_tx, ExecutionWsHealth::Ready);

    print_log(
        debug_mode,
        true,
        "[IBKR EXECUTION WS] READY | stream=order_updates".to_string(),
    );


    // ============================================================
    // ORDER UPDATE LOOP
    // ============================================================

    while let Some(item) = order_stream.next().await {
        match item {
            Ok(item) => {
                if tx_raw.send(item).is_err() {
                    set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);
                    print_log(debug_mode, true, "[IBKR EXECUTION WS] RAW UPDATE RECEIVER CLOSED".to_string());
                    return;
                }
            }

            Err(error) => {
                eprintln!("[IBKR EXECUTION WS] ORDER UPDATE STREAM ERROR | error={:?}", error);
            }
        }
    }


    // ============================================================
    // STREAM CLOSED
    // ============================================================

    set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);

    eprintln!("[IBKR EXECUTION WS] ORDER UPDATE STREAM CLOSED");
}


// ============================================================
// SET EXECUTION HEALTH
// ============================================================

fn set_execution_health(
    health_tx: &watch::Sender<ExecutionWsHealth>,
    health: ExecutionWsHealth,
) {
    let _ = health_tx.send(health);
}