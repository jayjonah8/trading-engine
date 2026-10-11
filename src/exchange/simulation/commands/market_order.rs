#![allow(dead_code)]

use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        fill::allocate_execution_id,
        order::Order,
        side::Side,
        ExecEvent,
        Fill,
        OrderType,
    },

    exchange::simulation::SharedSimulationMarketState,

    shared::print_log,
};


// ============================================================
// SIMULATION MARKET ORDER
// ============================================================

/// Simulates immediate market-order execution using live market data.
///
/// Market orders are filled against the latest available bid/ask quote:
///
/// - BUY  -> best ask
/// - SELL -> best bid
///
/// The simulation adapter never submits orders to a real exchange.
///
/// The Engine remains responsible for authoritative order state,
/// position accounting, trade lifecycle, and realized/unrealized PnL.
///
/// Initial implementation:
///
/// - market orders only
/// - immediate full fills
/// - bid/ask execution pricing
/// - live market timestamps
/// - optional stale quote validation
/// - no simulated fees yet
///
/// TODO:
/// - Add venue-specific trading fees.
/// - Add configurable slippage.
/// - Add simulated execution latency.
/// - Add available-liquidity checks.
/// - Add partial fills.
/// - Add limit-order execution.
/// - Add order cancellation and replacement.
pub async fn execute_market_order(
    order: Order,
    tx_exec_evt: &UnboundedSender<ExecEvent>,
    market_state: &SharedSimulationMarketState,
    debug_mode: bool,
) {
    // ============================================================
    // CONFIGURATION
    // ============================================================

    // Enable or disable live quote freshness validation.
    //
    // true  = reject quotes older than MAX_QUOTE_AGE_MS.
    // false = accept the latest stored quote regardless of age.
    const ENABLE_STALE_QUOTE_CHECK: bool = false;

    // Maximum permitted quote age in milliseconds.
    const MAX_QUOTE_AGE_MS: i64 = 5_000;


    // ============================================================
    // VALIDATE ORDER TYPE
    // ============================================================

    if order.order_type != OrderType::Market {
        let _ = tx_exec_evt.send(
            ExecEvent::OrderRejected {
                order_id: order.id,
                reason: format!(
                    "Simulation currently supports market orders only | order_type={:?}",
                    order.order_type,
                ),
                ts_ms: order.created_ts_ms,
            }
        );

        return;
    }


    // ============================================================
    // VALIDATE ORDER QUANTITY
    // ============================================================

    if !order.quantity.is_finite()
        || order.quantity <= 0.0
    {
        let _ = tx_exec_evt.send(
            ExecEvent::OrderRejected {
                order_id: order.id,
                reason: format!(
                    "Invalid simulation market-order quantity | symbol={} | qty={}",
                    order.symbol,
                    order.quantity,
                ),
                ts_ms: order.created_ts_ms,
            }
        );

        return;
    }


    // ============================================================
    // GET LATEST LIVE MARKET QUOTE
    // ============================================================

    // Clone the quote so the market-state read lock is released
    // before performing any execution processing.
    let quote = {
        let state =
            market_state.read().await;

        state.get_quote(
            &order.symbol
        )
        .cloned()
    };


    let Some(quote) =
        quote
    else {
        let _ = tx_exec_evt.send(
            ExecEvent::OrderRejected {
                order_id: order.id,
                reason: format!(
                    "No live market quote available for symbol={}",
                    order.symbol,
                ),
                ts_ms: order.created_ts_ms,
            }
        );

        return;
    };


    // ============================================================
    // VALIDATE LIVE MARKET QUOTE
    // ============================================================

    if !quote.bid_price.is_finite()
        || !quote.ask_price.is_finite()
        || quote.bid_price <= 0.0
        || quote.ask_price <= 0.0
        || quote.bid_price > quote.ask_price
        || !quote.bid_size.is_finite()
        || !quote.ask_size.is_finite()
        || quote.bid_size <= 0.0
        || quote.ask_size <= 0.0
    {
        let _ = tx_exec_evt.send(
            ExecEvent::OrderRejected {
                order_id: order.id,
                reason: format!(
                    "Invalid live market quote | symbol={} | bid={} | ask={}",
                    order.symbol,
                    quote.bid_price,
                    quote.ask_price,
                ),
                ts_ms: order.created_ts_ms,
            }
        );

        return;
    }


    // ============================================================
    // VALIDATE QUOTE FRESHNESS
    // ============================================================

    if ENABLE_STALE_QUOTE_CHECK {
        let now_ms =
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| {
                    duration.as_millis() as i64
                })
                .unwrap_or(0);

        let quote_age_ms =
            now_ms - quote.timestamp_ms;

        if quote_age_ms < 0
            || quote_age_ms > MAX_QUOTE_AGE_MS
        {
            let _ = tx_exec_evt.send(
                ExecEvent::OrderRejected {
                    order_id: order.id,
                    reason: format!(
                        "Stale live market quote | symbol={} | age_ms={} | max_age_ms={}",
                        order.symbol,
                        quote_age_ms,
                        MAX_QUOTE_AGE_MS,
                    ),
                    ts_ms: now_ms,
                }
            );

            return;
        }
    }


    // ============================================================
    // DETERMINE MARKET EXECUTION PRICE
    // ============================================================

    // Market BUY orders execute against the best available ask.
    // Market SELL orders execute against the best available bid.
    //
    // TODO:
    // Model market depth and slippage when order quantity exceeds
    // available top-of-book liquidity.
    let fill_price =
        match order.side {
            Side::Buy => {
                quote.ask_price
            }

            Side::Sell => {
                quote.bid_price
            }
        };


    // ============================================================
    // ORDER ACCEPTED
    // ============================================================

    // Simulation execution does not have a real venue-side order ID.
    // Generate an internal identifier to preserve the canonical
    // Engine execution lifecycle.
    let venue_order_id =
        format!(
            "SIMULATION-{}",
            order.id
        );

    if tx_exec_evt.send(
        ExecEvent::OrderAccepted {
            order_id: order.id,
            venue_order_id,
            ts_ms: quote.timestamp_ms,
        }
    ).is_err() {
        return;
    }


    // ============================================================
    // SIMULATED FULL FILL
    // ============================================================

    // V1 simulation execution model:
    //
    // - fill the entire requested quantity immediately
    // - BUY uses the current best ask
    // - SELL uses the current best bid
    // - preserve the configured execution venue
    // - no fees or slippage yet
    //
    // TODO:
    // Use actual simulated execution time instead of quote time
    // when latency modeling is introduced.
    let fill = Fill {
        execution_id: allocate_execution_id(),
        order_id: order.id,
        venue: order.venue,
        venue_execution_id: None,
        symbol: order.symbol.clone(),
        side: order.side,
        quantity: order.quantity,
        price: fill_price,
        fee: None,
        ts_ms: quote.timestamp_ms,
    };


    if tx_exec_evt.send(
        ExecEvent::Fill {
            fill,
        }
    ).is_err() {
        return;
    }


    // ============================================================
    // DEBUG: SIMULATION MARKET FILL
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[SIMULATION EXECUTION] MARKET FILL | order_id={} | symbol={} | side={:?} | qty={} | px={} | ts_ms={} | venue={:?}",
            order.id,
            order.symbol,
            order.side,
            order.quantity,
            fill_price,
            quote.timestamp_ms,
            order.venue,
        ),
    );
}