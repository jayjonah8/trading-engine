#![allow(dead_code)]

use ibapi::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{ExecEvent, OrderId},
    shared::print_log,
};


// ============================================================
// HANDLE CANCEL ORDER
// ============================================================

/// Handles a canonical IBKR cancellation request.
///
/// TODO:
/// Implement IBKR order cancellation after the main submit and execution
/// lifecycle is proven.
pub async fn handle_cancel_order(
    _client: &Client,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[IBKR ADAPTER] CANCEL NOT IMPLEMENTED | order_id={}",
            order_id,
        ),
    );
}