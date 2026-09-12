#![allow(dead_code)]

use chrono::Utc;

use hyperliquid_rust_sdk::{
    ExchangeClient,
    ExchangeDataStatus,
    ExchangeResponseStatus,
    MarketOrderParams,
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        fill::{
            ExecutionId,
            Fill,
        },
        ExecEvent,
        Order,
        OrderPurpose,
        Side,
    },
    shared::print_log,
};

// ============================================================
// HANDLE MARKET ORDER
// ============================================================

/// Submits a canonical market order to Hyperliquid.
///
/// The first implementation supports directional entry orders.
///
/// Immediate Hyperliquid responses are translated as:
///
/// Filled
///     -> OrderAccepted
///     -> Fill
///
/// Resting
///     -> OrderAccepted
///     -> OrderWorking
///
/// Explicit venue error
///     -> OrderRejected
///
/// SDK / transport failure
///     -> Error
///
/// Later, `execution_ws.rs` will subscribe to Hyperliquid `OrderUpdates`
/// and `UserEvents` so asynchronous lifecycle changes and fills are also
/// reported to the Engine.
pub async fn handle_market_order(
    exchange_client: &ExchangeClient,
    tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    // ============================================================
    // PURPOSE
    // ============================================================

    // For now, only directional market-entry orders are implemented.
    //
    // Market exits will later use the same canonical Order model with
    // OrderPurpose::Exit and reduce_only=true.
    if order.purpose != OrderPurpose::Entry {
        let ts_ms =
            Utc::now()
                .timestamp_millis();

        let message =
            format!(
                "Hyperliquid market order purpose not implemented: {:?}",
                order.purpose,
            );

        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ADAPTER] MARKET PURPOSE NOT IMPLEMENTED | order_id={} | purpose={:?}",
                order.id,
                order.purpose,
            ),
        );

        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message,
                    ts_ms,
                }
            );

        return;
    }

    // ============================================================
    // SIDE
    // ============================================================

    let is_buy =
        matches!(
            order.side,
            Side::Buy
        );

    // ============================================================
    // QUANTITY
    // ============================================================

    // TODO:
    // Normalize quantity using Hyperliquid asset metadata.
    //
    // The Engine owns the desired canonical quantity. Hyperliquid-specific
    // szDecimals / quantity-step requirements belong at this boundary.
    let quantity =
        order.quantity;

    // ============================================================
    // MARKET ORDER PARAMS
    // ============================================================

    let params =
        MarketOrderParams {
            asset: &order.symbol,
            is_buy,
            sz: quantity,
            px: None,

            // TODO:
            // Move Hyperliquid market-order slippage into venue-specific
            // execution configuration.
            slippage: Some(0.01),

            // TODO:
            // Encode the canonical client_order_id as a Hyperliquid CLOID
            // once the final CLOID format is implemented.
            cloid: None,

            wallet: None,
        };

    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] MARKET SUBMIT | order_id={} | symbol={} | side={:?} | qty={}",
            order.id,
            order.symbol,
            order.side,
            quantity,
        ),
    );

    // ============================================================
    // SUBMIT TO HYPERLIQUID
    // ============================================================

    let raw_response =
        match exchange_client
            .market_open(
                params
            )
            .await
        {
            Ok(response) => {
                response
            }

            Err(error) => {
                let ts_ms =
                    Utc::now()
                        .timestamp_millis();

                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID ADAPTER] MARKET SUBMIT ERROR | order_id={} | symbol={} | error={:?}",
                        order.id,
                        order.symbol,
                        error,
                    ),
                );

                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id: Some(order.id),
                            message: format!(
                                "Hyperliquid market_open failed: {:?}",
                                error,
                            ),
                            ts_ms,
                        }
                    );

                return;
            }
        };

    // ============================================================
    // VENUE RESPONSE
    // ============================================================

    let response =
        match raw_response {
            ExchangeResponseStatus::Ok(response) => {
                response
            }

            ExchangeResponseStatus::Err(error) => {
                let ts_ms =
                    Utc::now()
                        .timestamp_millis();

                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID ADAPTER] MARKET REJECTED | order_id={} | symbol={} | error={:?}",
                        order.id,
                        order.symbol,
                        error,
                    ),
                );

                let _ =
                    tx_evt.send(
                        ExecEvent::OrderRejected {
                            order_id: order.id,
                            reason: format!(
                                "{:?}",
                                error,
                            ),
                            ts_ms,
                        }
                    );

                return;
            }
        };

    // ============================================================
    // RESPONSE DATA
    // ============================================================

    let Some(data) =
        response.data
    else {
        let ts_ms =
            Utc::now()
                .timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ADAPTER] EMPTY MARKET RESPONSE | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );

        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message: "Hyperliquid response contained no data".to_string(),
                    ts_ms,
                }
            );

        return;
    };

    let Some(status) =
        data.statuses
            .first()
            .cloned()
    else {
        let ts_ms =
            Utc::now()
                .timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ADAPTER] MARKET RESPONSE HAS NO STATUS | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );

        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message: "Hyperliquid response contained no order status".to_string(),
                    ts_ms,
                }
            );

        return;
    };

    // ============================================================
    // IMMEDIATE STATUS
    // ============================================================

    match status {
        // ========================================================
        // FILLED
        // ========================================================

        ExchangeDataStatus::Filled(fill) => {
            let avg_px =
                match fill.avg_px
                    .parse::<f64>()
                {
                    Ok(value) => {
                        value
                    }

                    Err(error) => {
                        let ts_ms =
                            Utc::now()
                                .timestamp_millis();

                        print_log(
                            debug_mode,
                            true,
                            format!(
                                "[HYPERLIQUID ADAPTER] INVALID FILL PRICE | order_id={} | value={} | error={}",
                                order.id,
                                fill.avg_px,
                                error,
                            ),
                        );

                        let _ =
                            tx_evt.send(
                                ExecEvent::Error {
                                    order_id: Some(order.id),
                                    message: format!(
                                        "Invalid Hyperliquid fill price {}: {}",
                                        fill.avg_px,
                                        error,
                                    ),
                                    ts_ms,
                                }
                            );

                        return;
                    }
                };

            let total_sz =
                match fill.total_sz
                    .parse::<f64>()
                {
                    Ok(value) => {
                        value
                    }

                    Err(error) => {
                        let ts_ms =
                            Utc::now()
                                .timestamp_millis();

                        print_log(
                            debug_mode,
                            true,
                            format!(
                                "[HYPERLIQUID ADAPTER] INVALID FILL SIZE | order_id={} | value={} | error={}",
                                order.id,
                                fill.total_sz,
                                error,
                            ),
                        );

                        let _ =
                            tx_evt.send(
                                ExecEvent::Error {
                                    order_id: Some(order.id),
                                    message: format!(
                                        "Invalid Hyperliquid fill size {}: {}",
                                        fill.total_sz,
                                        error,
                                    ),
                                    ts_ms,
                                }
                            );

                        return;
                    }
                };

            let venue_order_id =
                fill.oid
                    .to_string();

            let ts_ms =
                Utc::now()
                    .timestamp_millis();

            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] MARKET FILLED | order_id={} | symbol={} | avg_px={} | total_sz={} | venue_order_id={}",
                    order.id,
                    order.symbol,
                    avg_px,
                    total_sz,
                    venue_order_id,
                ),
            );

            // The immediate Filled response proves the venue accepted the
            // order and assigned its venue-side order identifier.
            let _ =
                tx_evt.send(
                    ExecEvent::OrderAccepted {
                        order_id: order.id,
                        venue_order_id: venue_order_id.clone(),
                        ts_ms,
                    }
                );

            // TODO:
            // Hyperliquid's immediate Filled response is an order-level fill
            // summary rather than necessarily one individual venue execution.
            //
            // For this first market-order implementation it is represented as
            // one canonical Fill.
            //
            // Once `UserEvents` is wired in `execution_ws.rs`, use the real
            // venue execution identity and let FillLedger deduplicate anything
            // observed through both the immediate response and WebSocket.
            //
            // The temporary ExecutionId uses OrderId because only one immediate
            // summary Fill is currently created for a market order.
            let canonical_fill =
                Fill {
                    execution_id: ExecutionId(
                        order.id.as_u64()
                    ),
                    order_id: order.id,
                    venue: order.venue,
                    venue_execution_id: None,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    quantity: total_sz,
                    price: avg_px,
                    fee: None,
                    ts_ms,
                };

            let _ =
                tx_evt.send(
                    ExecEvent::Fill {
                        fill: canonical_fill,
                    }
                );
        }

        // ========================================================
        // RESTING
        // ========================================================

        ExchangeDataStatus::Resting(resting) => {
            let venue_order_id =
                resting.oid
                    .to_string();

            let ts_ms =
                Utc::now()
                    .timestamp_millis();

            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] MARKET RESTING | order_id={} | symbol={} | venue_order_id={}",
                    order.id,
                    order.symbol,
                    venue_order_id,
                ),
            );

            let _ =
                tx_evt.send(
                    ExecEvent::OrderAccepted {
                        order_id: order.id,
                        venue_order_id,
                        ts_ms,
                    }
                );

            let _ =
                tx_evt.send(
                    ExecEvent::OrderWorking {
                        order_id: order.id,
                        ts_ms,
                    }
                );
        }

        // ========================================================
        // OTHER STATUS
        // ========================================================

        other => {
            let ts_ms =
                Utc::now()
                    .timestamp_millis();

            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] UNKNOWN MARKET STATUS | order_id={} | symbol={} | status={:?}",
                    order.id,
                    order.symbol,
                    other,
                ),
            );

            let _ =
                tx_evt.send(
                    ExecEvent::Error {
                        order_id: Some(order.id),
                        message: format!(
                            "Unhandled Hyperliquid market-order status: {:?}",
                            other,
                        ),
                        ts_ms,
                    }
                );
        }
    }
}