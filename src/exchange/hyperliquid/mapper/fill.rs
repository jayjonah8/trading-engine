#![allow(dead_code)]

use hyperliquid_rust_sdk::TradeInfo;

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

    exchange::hyperliquid::{
        correlation::HyperliquidOrderCorrelation,

        mapper::client_order_id::{
            client_order_id_from_ws_cloid,
        },
    },

    shared::print_log,
};


// ============================================================
// MAP HYPERLIQUID FILL
// ============================================================

/// Converts one Hyperliquid `TradeInfo` execution into a canonical `Fill`.
///
/// Correlation path:
///
///     TradeInfo.cloid
///         ↓
///     ClientOrderId
///         ↓
///     HyperliquidOrderCorrelation
///         ↓
///     OrderId
///
/// Hyperliquid execution identity:
///
///     TradeInfo.tid
///         ↓
///     Fill.venue_execution_id
///
/// `execution_id` remains an engine-owned identity supplied by the caller.
pub fn map_trade_info_to_fill(
    trade: TradeInfo,
    correlation: &HyperliquidOrderCorrelation,
    execution_id: ExecutionId,
    debug_mode: bool,
) -> Option<Fill> {
    // ============================================================
    // CLOID
    // ============================================================

    let Some(cloid) =
        trade.cloid.as_deref()
    else {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID MAPPER] FILL MISSING CLOID | oid={} | tid={} | symbol={}",
                trade.oid,
                trade.tid,
                trade.coin,
            ),
        );

        return None;
    };


    // ============================================================
    // CLOID -> CLIENT ORDER ID
    // ============================================================

    let Some(client_order_id) =
        client_order_id_from_ws_cloid(
            cloid
        )
    else {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID MAPPER] INVALID FILL CLOID | cloid={} | oid={} | tid={}",
                cloid,
                trade.oid,
                trade.tid,
            ),
        );

        return None;
    };


    // ============================================================
    // CLIENT ORDER ID -> ORDER ID
    // ============================================================

    let Some(order_id): Option<OrderId> =
        correlation.get(
            client_order_id
        )
    else {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID MAPPER] FILL NOT CORRELATED | client_order_id={} | oid={} | tid={} | symbol={}",
                client_order_id,
                trade.oid,
                trade.tid,
                trade.coin,
            ),
        );

        return None;
    };


    // ============================================================
    // PRICE
    // ============================================================

    let price =
        match trade.px.parse::<f64>() {
            Ok(value)
                if value.is_finite()
                    && value > 0.0 =>
            {
                value
            }

            _ => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] INVALID FILL PRICE | order_id={} | tid={} | value={}",
                        order_id,
                        trade.tid,
                        trade.px,
                    ),
                );

                return None;
            }
        };


    // ============================================================
    // QUANTITY
    // ============================================================

    let quantity =
        match trade.sz.parse::<f64>() {
            Ok(value)
                if value.is_finite()
                    && value > 0.0 =>
            {
                value
            }

            _ => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] INVALID FILL QUANTITY | order_id={} | tid={} | value={}",
                        order_id,
                        trade.tid,
                        trade.sz,
                    ),
                );

                return None;
            }
        };


    // ============================================================
    // FEE
    // ============================================================

    let fee =
        trade.fee
            .parse::<f64>()
            .ok()
            .filter(
                |value| value.is_finite()
            );


    // ============================================================
    // SIDE
    // ============================================================

    let side =
        match trade.side.as_str() {
            "B" => {
                Side::Buy
            }

            "A" => {
                Side::Sell
            }

            other => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] INVALID FILL SIDE | order_id={} | tid={} | side={}",
                        order_id,
                        trade.tid,
                        other,
                    ),
                );

                return None;
            }
        };


    // ============================================================
    // TIMESTAMP
    // ============================================================

    let ts_ms =
        match i64::try_from(
            trade.time
        ) {
            Ok(value) => {
                value
            }

            Err(_) => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] INVALID FILL TIMESTAMP | order_id={} | tid={} | timestamp={}",
                        order_id,
                        trade.tid,
                        trade.time,
                    ),
                );

                return None;
            }
        };


    // ============================================================
    // CANONICAL FILL
    // ============================================================

    Some(
        Fill {
            execution_id,

            order_id,

            venue:
                Venue::Hyperliquid,

            venue_execution_id:
                Some(
                    trade.tid.to_string()
                ),

            symbol:
                trade.coin,

            side,

            quantity,

            price,

            fee,

            ts_ms,
        }
    )
}