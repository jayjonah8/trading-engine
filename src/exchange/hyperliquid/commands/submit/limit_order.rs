#![allow(dead_code)]

use chrono::Utc;

use hyperliquid_rust_sdk::{
    ClientLimit,
    ClientOrder,
    ClientOrderRequest,
    ExchangeClient,
    ExchangeDataStatus,
    ExchangeResponseStatus,
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        Order,
        Side,
        TimeInForce,
    },

    exchange::hyperliquid::mapper::to_hyperliquid_cloid,

    shared::print_log,
};


// This cleanly supports both Hyperliquid limit-order behaviors we proved
// experimentally:
//
// LIMIT ORDER
//     │
//     ├── Resting response
//     │      ↓
//     │   OrderAccepted
//     │      ↓
//     │   OrderWorking
//     │      ↓
//     │   OrderUpdates "open"
//     │      ↓
//     │   [possibly many UserEvents::Fills]
//     │      ↓
//     │   filled_quantity accumulates
//     │      ↓
//     │   Filled
//     │
//     └── Immediate Filled response
//            ↓
//        OrderAccepted
//            ↓
//        UserEvents::Fills
//            ↓
//        ExecEvent::Fill
//            ↓
//        filled_quantity accumulates
//            ↓
//        Filled
//
// Canonical resting-order lifecycle:
//
//     PendingSubmit
//          ↓
//     OrderAccepted
//          ↓
//       Working
//          ↓
//     [possibly many UserEvents::Fills]
//          ↓
//     filled_quantity accumulates
//          ↓
//        Filled
//
// No special Hyperliquid partial-fill logic is required in this submit
// handler. Each individual execution arrives through UserEvents::Fills and
// maps to ExecEvent::Fill for the same OrderId. The Engine's generic fill
// handler accumulates those executions until filled_quantity reaches the
// Order's requested quantity.
//
// There is also intentionally no OrderPurpose::Entry / OrderPurpose::Exit
// branching here. Unlike the current Hyperliquid market helper path, normal
// limit orders express their execution behavior through:
//
//     Side + reduce_only
//
// For example:
//
//     long entry  -> Buy  + reduce_only=false
//     long exit   -> Sell + reduce_only=true
//     short entry -> Sell + reduce_only=false
//     short exit  -> Buy  + reduce_only=true
//
// Keeping the limit-order handler purpose-independent also makes the same
// infrastructure reusable later for OrderPurpose::Quote and market-making
// orders without introducing a separate limit-order execution path.



// ============================================================
// HANDLE LIMIT ORDER
// ============================================================

/// Submits a canonical limit order to Hyperliquid.
///
/// Both entry and exit limit orders use Hyperliquid's normal `order()` path.
///
/// Examples:
///
/// Entry long:
///
///     Side::Buy
///     reduce_only = false
///
/// Exit long:
///
///     Side::Sell
///     reduce_only = true
///
/// Entry short:
///
///     Side::Sell
///     reduce_only = false
///
/// Exit short:
///
///     Side::Buy
///     reduce_only = true
///
/// Immediate SDK responses are used only for submission/lifecycle facts:
///
/// Filled
///     -> OrderAccepted
///
/// Resting
///     -> OrderAccepted
///     -> OrderWorking
///
/// Explicit venue rejection
///     -> OrderRejected
///
/// SDK / transport failure
///     -> Error
///
/// IMPORTANT:
///
/// The immediate Hyperliquid response does NOT create canonical `Fill`s.
///
/// Actual individual executions come exclusively from:
///
///     UserEvents::Fills
///         ↓
///     mapper/user_event.rs
///         ↓
///     mapper/fill.rs
///         ↓
///     ExecEvent::Fill
///
/// `OrderUpdates` independently reports lifecycle facts such as:
///
///     open
///     canceled
///     filled
pub async fn handle_limit_order(
    exchange_client: &ExchangeClient,
    tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    // ============================================================
    // LIMIT PRICE
    // ============================================================

    let Some(limit_px) =
        order.limit_px
    else {
        let ts_ms =
            Utc::now()
                .timestamp_millis();


        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ADAPTER] LIMIT ORDER MISSING PRICE | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );


        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),

                    message:
                        "Hyperliquid limit order missing limit_px"
                            .to_string(),

                    ts_ms,
                }
            );


        return;
    };


    // ============================================================
    // SIDE
    // ============================================================

    let is_buy =
        matches!(
            order.side,
            Side::Buy
        );


    // ============================================================
    // TIME IN FORCE
    // ============================================================

    let tif =
        match order.time_in_force {
            TimeInForce::Gtc => {
                "Gtc"
            }

            TimeInForce::Ioc => {
                "Ioc"
            }

            // Hyperliquid support/mapping for these canonical policies should
            // be added only after their exact venue behavior is confirmed.
            TimeInForce::Fok => {
                let ts_ms =
                    Utc::now()
                        .timestamp_millis();


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id: Some(order.id),

                            message:
                                "Hyperliquid FOK limit orders are not implemented"
                                    .to_string(),

                            ts_ms,
                        }
                    );


                return;
            }

            TimeInForce::Day => {
                let ts_ms =
                    Utc::now()
                        .timestamp_millis();


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id: Some(order.id),

                            message:
                                "Hyperliquid Day limit orders are not implemented"
                                    .to_string(),

                            ts_ms,
                        }
                    );


                return;
            }
        };


    // ============================================================
    // POST ONLY
    // ============================================================

    // Hyperliquid represents post-only limit orders with the Alo TIF.
    //
    // Therefore a canonical post-only order overrides normal GTC behavior.
    //
    // We currently reject post_only + IOC because those policies conflict:
    //
    // post_only
    //     -> must NOT immediately execute
    //
    // IOC
    //     -> immediately execute available quantity and cancel remainder
    let tif =
        if order.post_only {
            if order.time_in_force != TimeInForce::Gtc {
                let ts_ms =
                    Utc::now()
                        .timestamp_millis();


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id: Some(order.id),

                            message:
                                format!(
                                    "Hyperliquid post_only currently requires TimeInForce::Gtc, got {:?}",
                                    order.time_in_force,
                                ),

                            ts_ms,
                        }
                    );


                return;
            }


            "Alo"
        } else {
            tif
        };


    // ============================================================
    // CLOID
    // ============================================================

    let cloid =
        to_hyperliquid_cloid(
            order.client_order_id
        );


    // ============================================================
    // REQUEST
    // ============================================================

    let request =
        ClientOrderRequest {
            asset:
                order.symbol.clone(),

            is_buy,

            reduce_only:
                order.reduce_only,

            limit_px,

            sz:
                order.quantity,

            cloid:
                Some(cloid),

            order_type:
                ClientOrder::Limit(
                    ClientLimit {
                        tif:
                            tif.to_string(),
                    }
                ),
        };


    // ============================================================
    // SUBMIT
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] LIMIT SUBMIT | order_id={} | client_order_id={} | purpose={:?} | symbol={} | side={:?} | qty={} | limit_px={} | tif={} | reduce_only={} | post_only={}",
            order.id,
            order.client_order_id,
            order.purpose,
            order.symbol,
            order.side,
            order.quantity,
            limit_px,
            tif,
            order.reduce_only,
            order.post_only,
        ),
    );


    let raw_response =
        match exchange_client
            .order(
                request,
                None,
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
                        "[HYPERLIQUID ADAPTER] LIMIT SUBMIT ERROR | order_id={} | symbol={} | error={:?}",
                        order.id,
                        order.symbol,
                        error,
                    ),
                );


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id: Some(order.id),

                            message:
                                format!(
                                    "Hyperliquid limit order submission failed: {:?}",
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
                        "[HYPERLIQUID ADAPTER] LIMIT REJECTED | order_id={} | symbol={} | error={:?}",
                        order.id,
                        order.symbol,
                        error,
                    ),
                );


                let _ =
                    tx_evt.send(
                        ExecEvent::OrderRejected {
                            order_id:
                                order.id,

                            reason:
                                format!(
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
                "[HYPERLIQUID ADAPTER] EMPTY LIMIT RESPONSE | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );


        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),

                    message:
                        "Hyperliquid limit response contained no data"
                            .to_string(),

                    ts_ms,
                }
            );


        return;
    };


    // ============================================================
    // RESPONSE STATUS
    // ============================================================

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
                "[HYPERLIQUID ADAPTER] LIMIT RESPONSE HAS NO STATUS | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );


        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),

                    message:
                        "Hyperliquid limit response contained no order status"
                            .to_string(),

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

        ExchangeDataStatus::Filled(filled) => {
            let venue_order_id =
                filled.oid
                    .to_string();


            let ts_ms =
                Utc::now()
                    .timestamp_millis();


            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] LIMIT IMMEDIATE FILLED | order_id={} | symbol={} | purpose={:?} | venue_order_id={} | avg_px={} | total_sz={} | fill_source=UserEvents",
                    order.id,
                    order.symbol,
                    order.purpose,
                    venue_order_id,
                    filled.avg_px,
                    filled.total_sz,
                ),
            );


            // The immediate response proves that Hyperliquid accepted the
            // order and assigned an OID.
            //
            // Do NOT create a canonical Fill here.
            //
            // UserEvents::Fills remains the authoritative execution source.
            let _ =
                tx_evt.send(
                    ExecEvent::OrderAccepted {
                        order_id:
                            order.id,

                        venue_order_id,

                        ts_ms,
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
                    "[HYPERLIQUID ADAPTER] LIMIT RESTING | order_id={} | symbol={} | purpose={:?} | venue_order_id={} | limit_px={}",
                    order.id,
                    order.symbol,
                    order.purpose,
                    venue_order_id,
                    limit_px,
                ),
            );


            let _ =
                tx_evt.send(
                    ExecEvent::OrderAccepted {
                        order_id:
                            order.id,

                        venue_order_id,

                        ts_ms,
                    }
                );


            let _ =
                tx_evt.send(
                    ExecEvent::OrderWorking {
                        order_id:
                            order.id,

                        ts_ms,
                    }
                );
        }


        // ========================================================
        // EXPLICIT STATUS ERROR
        // ========================================================

        ExchangeDataStatus::Error(error) => {
            let ts_ms =
                Utc::now()
                    .timestamp_millis();


            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] LIMIT REJECTED | order_id={} | symbol={} | error={}",
                    order.id,
                    order.symbol,
                    error,
                ),
            );


            let _ =
                tx_evt.send(
                    ExecEvent::OrderRejected {
                        order_id:
                            order.id,

                        reason:
                            error,

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
                    "[HYPERLIQUID ADAPTER] UNHANDLED LIMIT STATUS | order_id={} | symbol={} | status={:?}",
                    order.id,
                    order.symbol,
                    other,
                ),
            );


            let _ =
                tx_evt.send(
                    ExecEvent::Error {
                        order_id:
                            Some(order.id),

                        message:
                            format!(
                                "Unhandled Hyperliquid limit-order status: {:?}",
                                other,
                            ),

                        ts_ms,
                    }
                );
        }
    }
}

