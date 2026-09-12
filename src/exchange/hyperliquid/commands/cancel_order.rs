#![allow(dead_code)]

use hyperliquid_rust_sdk::ExchangeClient;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        OrderId,
    },

    shared::print_log,
};


/// Handles a canonical Hyperliquid order cancellation request.
///
/// TODO:
/// Implement after the basic market submit/fill lifecycle is proven.
///
/// Cancellation will require the Hyperliquid venue-side order identifier,
/// not only the engine's internal `OrderId`.
pub async fn handle_cancel_order(
    _exchange_client: &ExchangeClient,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] CANCEL NOT IMPLEMENTED | order_id={}",
            order_id,
        ),
    );
}