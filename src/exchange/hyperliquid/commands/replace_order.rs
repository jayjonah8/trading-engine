#![allow(dead_code)]

use hyperliquid_rust_sdk::ExchangeClient;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        OrderChanges,
        OrderId,
    },

    shared::print_log,
};


/// Handles a canonical Hyperliquid replacement/modification request.
///
/// TODO:
/// Implement after submit and cancellation behavior are proven.
///
/// The adapter will translate `OrderChanges` into Hyperliquid's native
/// modification mechanism while preserving canonical `OrderId` identity.
pub async fn handle_replace_order(
    _exchange_client: &ExchangeClient,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    changes: OrderChanges,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] REPLACE NOT IMPLEMENTED | order_id={} | quantity={:?} | limit_px={:?} | stop_px={:?}",
            order_id,
            changes.quantity,
            changes.limit_px,
            changes.stop_px,
        ),
    );
}