#![allow(dead_code)]

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        OrderChanges,
        OrderId,
    },

    exchange::alpaca::types::AlpacaConfig,

    shared::print_log,
};


// ============================================================
// HANDLE REPLACE ORDER
// ============================================================

/// Handles a canonical Alpaca replacement/modification request.
///
/// TODO:
/// Implement after submit and cancellation behavior are proven.
///
/// The adapter will translate `OrderChanges` into Alpaca's native order
/// replacement mechanism while preserving the canonical Engine `OrderId`.
pub async fn handle_replace_order(
    _alpaca_config: &AlpacaConfig,
    _tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    changes: OrderChanges,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] REPLACE NOT IMPLEMENTED | order_id={} | quantity={:?} | limit_px={:?} | stop_px={:?}",
            order_id,
            changes.quantity,
            changes.limit_px,
            changes.stop_px,
        ),
    );
}