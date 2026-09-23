#![allow(dead_code)]

use chrono::Utc;
use ibapi::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{ExecEvent, OrderId},
    shared::print_log,
};


// ============================================================
// HANDLE CANCEL ORDER
// ============================================================

/// Sends a cancellation request for an existing IBKR order.
///
/// The adapter resolves the canonical `OrderId` into its IBKR order ID before
/// calling this function.
///
/// Successful `cancel_order()` only means the cancellation request was sent.
/// The IBKR order-update stream remains authoritative for cancellation
/// confirmation through `OrderStatusKind::Cancelled`.
pub async fn handle_cancel_order(
    client: &Client,
    tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    ibkr_order_id: i32,
    debug_mode: bool,
) {
    // ============================================================
    // CANCEL
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[IBKR ADAPTER] CANCEL REQUEST | order_id={} | ibkr_order_id={}",
            order_id, ibkr_order_id,
        ),
    );

    let result = client.cancel_order(ibkr_order_id, "").await;


    // ============================================================
    // RESULT
    // ============================================================

    match result {
        Ok(_cancel_stream) => {
            // Do not wait on or consume the returned cancellation stream here.
            //
            // The persistent order-update stream is authoritative for the
            // canonical cancellation lifecycle.
            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR ADAPTER] CANCEL SENT | order_id={} | ibkr_order_id={}",
                    order_id, ibkr_order_id,
                ),
            );
        }

        Err(error) => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR ADAPTER] CANCEL ERROR | order_id={} | ibkr_order_id={} | error={:?}",
                    order_id, ibkr_order_id, error,
                ),
            );

            let _ = tx_evt.send(ExecEvent::Error {
                order_id: Some(order_id),
                message: format!("IBKR cancellation request failed: {:?}", error),
                ts_ms: Utc::now().timestamp_millis(),
            });
        }
    }
}