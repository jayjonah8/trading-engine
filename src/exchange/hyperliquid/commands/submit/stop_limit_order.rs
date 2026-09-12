#![allow(dead_code)]

use hyperliquid_rust_sdk::ExchangeClient;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        Order,
    },
    shared::print_log,
};

pub async fn handle_stop_limit_order(
    _exchange_client: &ExchangeClient,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] STOP LIMIT NOT IMPLEMENTED | order_id={} | symbol={}",
            order.id,
            order.symbol,
        ),
    );

    // TODO:
    // Implement canonical StopLimit -> Hyperliquid trigger-limit
    // order translation.
}