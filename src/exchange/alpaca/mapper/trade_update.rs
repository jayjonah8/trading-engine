#![allow(dead_code)]

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        fill::allocate_execution_id,
        ExecEvent,
        OrderId,
    },

    exchange::alpaca::{
        correlation::AlpacaOrderCorrelation,

        mapper::{
            client_order_id::client_order_id_from_alpaca,
            fill::map_alpaca_execution_to_fill,
        },
    },

    shared::print_log,
};


// ============================================================
// ALPACA TRADE UPDATE
// ============================================================

/// Production representation of one Alpaca `trade_updates` event.
///
/// Alpaca uses the same stream for both:
///
/// - order lifecycle events
/// - individual execution/fill events
///
/// Execution-specific fields such as `price`, `qty`, and `execution_id` are
/// optional because lifecycle events such as `pending_new` do not contain
/// execution data.
#[derive(Debug, Deserialize)]
struct AlpacaTradeUpdate {
    event: String,
    event_id: Option<String>,
    timestamp: String,
    order: AlpacaTradeUpdateOrder,
    price: Option<String>,
    qty: Option<String>,
    position_qty: Option<String>,
    execution_id: Option<String>,
}


// ============================================================
// ALPACA TRADE UPDATE ORDER
// ============================================================

/// Order snapshot embedded inside an Alpaca `trade_updates` event.
///
/// `filled_qty` and `filled_avg_price` represent cumulative order state.
///
/// They must NOT be interpreted as the quantity and price of the current
/// individual execution.
#[derive(Debug, Deserialize)]
struct AlpacaTradeUpdateOrder {
    id: String,
    client_order_id: String,
    symbol: String,
    filled_qty: String,
    filled_avg_price: Option<String>,
    side: String,
    status: String,
}


// ============================================================
// HANDLE TRADE UPDATE
// ============================================================

/// Translates one raw Alpaca `trade_updates` WebSocket message into canonical
/// Engine execution events.
///
/// Correlation:
///
///     Alpaca client_order_id
///         ↓
///     ClientOrderId
///         ↓
///     AlpacaOrderCorrelation
///         ↓
///     OrderId
///
/// Fill:
///
///     partial_fill / fill
///         ↓
///     top-level qty + price + execution_id
///         ↓
///     canonical Fill
///         ↓
///     ExecEvent::Fill
///
/// IMPORTANT:
///
/// `order.filled_qty` and `order.filled_avg_price` are cumulative.
///
/// The top-level `qty`, `price`, and `execution_id` describe the individual
/// execution and are used to construct canonical `Fill`s.
pub fn handle_trade_update(
    value: Value,
    correlation: &AlpacaOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    // ============================================================
    // DATA
    // ============================================================

    let Some(data) = value.get("data").cloned() else {
        print_log(debug_mode, true, "[ALPACA MAPPER] TRADE UPDATE MISSING DATA");
        return;
    };


    // ============================================================
    // DESERIALIZE
    // ============================================================

    let update: AlpacaTradeUpdate = match serde_json::from_value(data) {
        Ok(update) => update,

        Err(error) => {
            print_log(
                debug_mode,
                true,
                format!("[ALPACA MAPPER] TRADE UPDATE DESERIALIZE FAILED | error={}", error),
            );

            return;
        }
    };


    // ============================================================
    // RESOLVE ORDER ID
    // ============================================================

    let Some(order_id) = resolve_order_id(
        &update.order.client_order_id,
        &update.order.id,
        correlation,
        debug_mode,
    ) else {
        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA MAPPER] TRADE UPDATE NOT CORRELATED | venue_order_id={} | client_order_id={} | symbol={} | event={} | status={}",
                update.order.id,
                update.order.client_order_id,
                update.order.symbol,
                update.event,
                update.order.status,
            ),
        );

        return;
    };


    // ============================================================
    // STATUS
    // ============================================================

    match update.event.as_str() {
        // ========================================================
        // PENDING NEW
        // ========================================================

        "pending_new" => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] ORDER PENDING NEW | order_id={} | venue_order_id={} | symbol={}",
                    order_id,
                    update.order.id,
                    update.order.symbol,
                ),
            );

            // Engine already owns PendingSubmit after issuing SubmitOrder.
        }


        // ========================================================
        // NEW / ACCEPTED
        // ========================================================

        "new" | "accepted" => {
            let Some(ts_ms) = parse_timestamp_ms(&update.timestamp, order_id, debug_mode) else {
                return;
            };

            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] ORDER WORKING | order_id={} | venue_order_id={} | symbol={} | event={}",
                    order_id,
                    update.order.id,
                    update.order.symbol,
                    update.event,
                ),
            );

            let _ = tx_evt.send(ExecEvent::OrderWorking { order_id, ts_ms });
        }


        // ========================================================
        // PARTIAL FILL / FINAL FILL
        // ========================================================

        "partial_fill" | "fill" => {
            handle_fill(update, order_id, tx_evt, debug_mode);
        }


        // ========================================================
        // CANCELED
        // ========================================================

        "canceled" => {
            let Some(ts_ms) = parse_timestamp_ms(&update.timestamp, order_id, debug_mode) else {
                return;
            };

            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] ORDER CANCELED | order_id={} | venue_order_id={} | symbol={}",
                    order_id,
                    update.order.id,
                    update.order.symbol,
                ),
            );

            let _ = tx_evt.send(ExecEvent::OrderCanceled { order_id, ts_ms });
        }


        // ========================================================
        // EXPIRED
        // ========================================================

        "expired" => {
            let Some(ts_ms) = parse_timestamp_ms(&update.timestamp, order_id, debug_mode) else {
                return;
            };

            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] ORDER EXPIRED | order_id={} | venue_order_id={} | symbol={}",
                    order_id,
                    update.order.id,
                    update.order.symbol,
                ),
            );

            let _ = tx_evt.send(ExecEvent::OrderExpired { order_id, ts_ms });
        }


        // ========================================================
        // REJECTED
        // ========================================================

        "rejected" => {
            let Some(ts_ms) = parse_timestamp_ms(&update.timestamp, order_id, debug_mode) else {
                return;
            };

            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] ORDER REJECTED | order_id={} | venue_order_id={} | symbol={}",
                    order_id,
                    update.order.id,
                    update.order.symbol,
                ),
            );

            let _ = tx_evt.send(ExecEvent::OrderRejected {
                order_id,
                reason: format!("Alpaca rejected order {}", update.order.id),
                ts_ms,
            });
        }


        // ========================================================
        // OTHER
        // ========================================================

        other => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] UNHANDLED TRADE UPDATE | order_id={} | venue_order_id={} | symbol={} | event={} | status={}",
                    order_id,
                    update.order.id,
                    update.order.symbol,
                    other,
                    update.order.status,
                ),
            );
        }
    }
}


// ============================================================
// HANDLE FILL
// ============================================================

fn handle_fill(
    update: AlpacaTradeUpdate,
    order_id: OrderId,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    // ============================================================
    // EXECUTION PRICE
    // ============================================================

    let Some(price) = update.price.as_deref() else {
        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA MAPPER] FILL MISSING PRICE | order_id={} | venue_order_id={} | event={}",
                order_id,
                update.order.id,
                update.event,
            ),
        );

        return;
    };


    // ============================================================
    // EXECUTION QUANTITY
    // ============================================================

    let Some(quantity) = update.qty.as_deref() else {
        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA MAPPER] FILL MISSING QUANTITY | order_id={} | venue_order_id={} | event={}",
                order_id,
                update.order.id,
                update.event,
            ),
        );

        return;
    };


    // ============================================================
    // VENUE EXECUTION ID
    // ============================================================

    let Some(venue_execution_id) = update.execution_id.clone() else {
        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA MAPPER] FILL MISSING EXECUTION ID | order_id={} | venue_order_id={} | event={}",
                order_id,
                update.order.id,
                update.event,
            ),
        );

        return;
    };


    // ============================================================
    // CANONICAL EXECUTION ID
    // ============================================================

    let execution_id = allocate_execution_id();


    // ============================================================
    // MAP EXECUTION -> FILL
    // ============================================================

    let Some(fill) = map_alpaca_execution_to_fill(
        order_id,
        update.order.symbol.clone(),
        &update.order.side,
        price,
        quantity,
        venue_execution_id,
        &update.timestamp,
        execution_id,
        debug_mode,
    ) else {
        return;
    };


    // ============================================================
    // LOG
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA MAPPER] FILL MAPPED | order_id={} | execution_id={:?} | venue_execution_id={:?} | symbol={} | side={:?} | qty={} | price={} | cumulative_qty={} | cumulative_avg_px={:?} | event={} | ts_ms={}",
            fill.order_id,
            fill.execution_id,
            fill.venue_execution_id,
            fill.symbol,
            fill.side,
            fill.quantity,
            fill.price,
            update.order.filled_qty,
            update.order.filled_avg_price,
            update.event,
            fill.ts_ms,
        ),
    );


    // ============================================================
    // EMIT CANONICAL FILL
    // ============================================================

    let _ = tx_evt.send(ExecEvent::Fill { fill });
}


// ============================================================
// RESOLVE CANONICAL ORDER ID
// ============================================================

fn resolve_order_id(
    alpaca_client_order_id: &str,
    venue_order_id: &str,
    correlation: &AlpacaOrderCorrelation,
    debug_mode: bool,
) -> Option<OrderId> {
    // ============================================================
    // CLIENT ORDER ID
    // ============================================================

    if let Some(client_order_id) = client_order_id_from_alpaca(alpaca_client_order_id) {
        if let Some(order_id) = correlation.get_by_client_order_id(client_order_id) {
            return Some(order_id);
        }
    }


    // ============================================================
    // VENUE ORDER ID FALLBACK
    // ============================================================

    if let Some(order_id) = correlation.get_by_venue_order_id(venue_order_id) {
        return Some(order_id);
    }


    // ============================================================
    // FAILED
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA MAPPER] UNKNOWN ORDER | client_order_id={} | venue_order_id={}",
            alpaca_client_order_id,
            venue_order_id,
        ),
    );

    None
}


// ============================================================
// PARSE ALPACA TIMESTAMP
// ============================================================

/// Parses an Alpaca venue timestamp.
///
/// Invalid venue timestamps are rejected rather than replaced with the local
/// system time. Canonical execution state should never contain an invented
/// timestamp that makes malformed venue data appear valid.
fn parse_timestamp_ms(
    timestamp: &str,
    order_id: OrderId,
    debug_mode: bool,
) -> Option<i64> {
    match chrono::DateTime::parse_from_rfc3339(timestamp) {
        Ok(value) => Some(value.timestamp_millis()),

        Err(error) => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA MAPPER] INVALID TIMESTAMP | order_id={} | timestamp={} | error={}",
                    order_id,
                    timestamp,
                    error,
                ),
            );

            None
        }
    }
}