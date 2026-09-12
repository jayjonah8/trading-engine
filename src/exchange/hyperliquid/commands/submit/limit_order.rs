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

pub async fn handle_limit_order(
    _exchange_client: &ExchangeClient,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] LIMIT ORDER NOT IMPLEMENTED | order_id={} | symbol={}",
            order.id,
            order.symbol,
        ),
    );

    // TODO:
    // Implement canonical Limit -> Hyperliquid limit-order translation.
    //
    // This will need to support resting orders and later lifecycle updates
    // from execution_ws.rs.
}