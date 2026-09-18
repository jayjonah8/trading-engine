#![allow(dead_code)]

use chrono::DateTime;

use crate::{
    core::execution::{
        fill::{
            ExecutionId,
            Fill,
        },
        OrderId,
        Side,
        Venue,
    },

    shared::print_log,
};


// ============================================================
// MAP ALPACA FILL
// ============================================================

/// Converts one Alpaca execution from `trade_updates` into a canonical `Fill`.
///
/// Alpaca exposes both individual execution data and cumulative order data.
///
/// Individual execution data:
///
///     price
///     qty
///     execution_id
///     timestamp
///
/// Cumulative order data:
///
///     order.filled_qty
///     order.filled_avg_price
///
/// IMPORTANT:
///
/// Only the individual execution fields belong in the canonical `Fill`.
///
/// `order.filled_qty` and `order.filled_avg_price` must NOT be used here
/// because they already include previous executions.
///
/// Example:
///
///     previous cumulative filled_qty = 40
///     current execution qty          = 74
///     new cumulative filled_qty      = 114
///
/// The canonical Fill quantity for that event is 74, not 114.
///
/// `execution_id` remains an Engine-owned identity supplied by the caller.
/// Alpaca's execution ID is preserved separately as `venue_execution_id`.
pub fn map_alpaca_execution_to_fill(
    order_id: OrderId,
    symbol: String,
    side: &str,
    price: &str,
    quantity: &str,
    venue_execution_id: String,
    timestamp: &str,
    execution_id: ExecutionId,
    debug_mode: bool,
) -> Option<Fill> {
    // ============================================================
    // PRICE
    // ============================================================

    let price = match price.parse::<f64>() {
        Ok(value) if value.is_finite() && value > 0.0 => value,

        _ => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] INVALID FILL PRICE | order_id={} | venue_execution_id={} | value={}",
                    order_id,
                    venue_execution_id,
                    price,
                ),
            );

            return None;
        }
    };


    // ============================================================
    // QUANTITY
    // ============================================================

    let quantity = match quantity.parse::<f64>() {
        Ok(value) if value.is_finite() && value > 0.0 => value,

        _ => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] INVALID FILL QUANTITY | order_id={} | venue_execution_id={} | value={}",
                    order_id,
                    venue_execution_id,
                    quantity,
                ),
            );

            return None;
        }
    };


    // ============================================================
    // SIDE
    // ============================================================

    let side = match side {
        "buy" => Side::Buy,
        "sell" => Side::Sell,

        other => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] INVALID FILL SIDE | order_id={} | venue_execution_id={} | side={}",
                    order_id,
                    venue_execution_id,
                    other,
                ),
            );

            return None;
        }
    };


    // ============================================================
    // TIMESTAMP
    // ============================================================

    let ts_ms = match DateTime::parse_from_rfc3339(timestamp) {
        Ok(value) => value.timestamp_millis(),

        Err(error) => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] INVALID FILL TIMESTAMP | order_id={} | venue_execution_id={} | timestamp={} | error={}",
                    order_id,
                    venue_execution_id,
                    timestamp,
                    error,
                ),
            );

            return None;
        }
    };


    // ============================================================
    // CANONICAL FILL
    // ============================================================

    Some(Fill {
        execution_id,
        order_id,
        venue: Venue::Alpaca,
        venue_execution_id: Some(venue_execution_id),
        symbol,
        side,
        quantity,
        price,
        fee: None,
        ts_ms,
    })
}