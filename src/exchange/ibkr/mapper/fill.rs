#![allow(dead_code)]

use crate::{
    core::execution::{
        fill::{allocate_execution_id, Fill},
        OrderId, Side, Venue,
    },
    shared::print_log,
};


// ============================================================
// MAP IBKR EXECUTION TO FILL
// ============================================================

/// Converts one individual IBKR execution into a canonical `Fill`.
///
/// Each individual IBKR `ExecutionData` event becomes exactly one canonical
/// Fill. `Execution.shares` is the quantity of this individual execution.
///
/// Do NOT use:
///
///     cumulative_quantity
///     average_price
///
/// as the individual Fill quantity/price.
///
/// IBKR's execution ID is preserved as `venue_execution_id`.
///
/// Commission is intentionally left as `None` because IBKR reports commission
/// separately through `CommissionReport`.
///
/// TODO:
/// `ts_ms` currently comes from local receipt time in `order_update.rs`.
/// Once the exact runtime format of `Execution.time` is confirmed, parse the
/// IBKR execution timestamp there and pass that venue timestamp here instead.
pub fn map_ibkr_execution_to_fill(
    order_id: OrderId,
    symbol: String,
    side: Side,
    quantity: f64,
    price: f64,
    venue_execution_id: String,
    ts_ms: i64,
    debug_mode: bool,
) -> Option<Fill> {
    // ============================================================
    // QUANTITY
    // ============================================================

    if !quantity.is_finite() || quantity <= 0.0 {
        print_log(
            debug_mode,
            true,
            format!(
                "[IBKR MAPPER] INVALID FILL QUANTITY | order_id={} | venue_execution_id={} | quantity={}",
                order_id, venue_execution_id, quantity,
            ),
        );

        return None;
    }


    // ============================================================
    // PRICE
    // ============================================================

    if !price.is_finite() || price <= 0.0 {
        print_log(
            debug_mode,
            true,
            format!(
                "[IBKR MAPPER] INVALID FILL PRICE | order_id={} | venue_execution_id={} | price={}",
                order_id, venue_execution_id, price,
            ),
        );

        return None;
    }


    // ============================================================
    // VENUE EXECUTION ID
    // ============================================================

    if venue_execution_id.is_empty() {
        print_log(
            debug_mode,
            true,
            format!("[IBKR MAPPER] EMPTY VENUE EXECUTION ID | order_id={}", order_id),
        );

        return None;
    }


    // ============================================================
    // CANONICAL EXECUTION ID
    // ============================================================

    let execution_id = allocate_execution_id();


    // ============================================================
    // CANONICAL FILL
    // ============================================================

    Some(Fill {
        execution_id,
        order_id,
        venue: Venue::InteractiveBrokers,
        venue_execution_id: Some(venue_execution_id),
        symbol,
        side,
        quantity,
        price,
        fee: None,
        ts_ms,
    })
}