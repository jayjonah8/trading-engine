#![allow(dead_code)]

use ibapi::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{ExecEvent, Order},
    shared::print_log,
};


// ============================================================
// HANDLE STOP LIMIT ORDER
// ============================================================

pub async fn handle_stop_limit_order(
    _client: &Client,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[IBKR ADAPTER] STOP LIMIT NOT IMPLEMENTED | order_id={} | symbol={}",
            order.id,
            order.symbol,
        ),
    );

    // TODO:
    // Implement canonical StopLimit -> IBKR stop-limit order translation.
}