#![allow(dead_code)]

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        Order,
    },

    exchange::alpaca::types::AlpacaConfig,

    shared::print_log,
};


// ============================================================
// HANDLE STOP LIMIT ORDER
// ============================================================

pub async fn handle_stop_limit_order(
    _alpaca_config: &AlpacaConfig,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] STOP LIMIT NOT IMPLEMENTED | order_id={} | symbol={}",
            order.id,
            order.symbol,
        ),
    );

    // TODO:
    // Implement canonical StopLimit -> Alpaca stop-limit order translation.
}