#![allow(dead_code)]

use chrono::Utc;

use hyperliquid_rust_sdk::{
    ClientCancelRequestCloid,
    ExchangeClient,
    ExchangeDataStatus,
    ExchangeResponseStatus,
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        OrderId,
    },

    exchange::hyperliquid::{
        correlation::HyperliquidOrderRef,
        mapper::to_hyperliquid_cloid,
    },

    shared::print_log,
};


// ============================================================
// HANDLE CANCEL ORDER
// ============================================================

/// Requests cancellation of an existing Hyperliquid order.
///
/// The adapter resolves the canonical `OrderId` into an owned
/// `HyperliquidOrderRef` BEFORE spawning this asynchronous request.
///
/// Resolution occurs in `adapter.rs`:
///
///     OrderId
///         ↓
///     HyperliquidOrderCorrelation
///         ↓
///     HyperliquidOrderRef
///         ├── ClientOrderId
///         └── symbol
///         ↓
///     spawned cancellation task
///         ↓
///     ClientOrderId -> CLOID
///         ↓
///     cancel_by_cloid(...)
///
/// This keeps `HyperliquidOrderCorrelation` owned exclusively by the main
/// adapter event loop. The spawned cancellation task receives only the
/// transport information it actually needs.
///
/// IMPORTANT:
///
/// A successful synchronous cancel response means Hyperliquid accepted
/// the cancellation request.
///
/// It does NOT directly emit `ExecEvent::OrderCanceled`.
///
/// The authoritative asynchronous lifecycle confirmation comes from:
///
///     OrderUpdates
///         ↓
///     status = "canceled"
///         ↓
///     mapper/order_update.rs
///         ↓
///     ExecEvent::OrderCanceled
///         ↓
///     Engine
///
/// This preserves the asynchronous cancellation lifecycle:
///
///     Working
///         ↓
///     PendingCancel
///         ↓
///     ExecCommand::CancelOrder
///         ↓
///     adapter resolves HyperliquidOrderRef
///         ↓
///     Hyperliquid cancel request
///         ↓
///     OrderUpdates "canceled"
///         ↓
///     ExecEvent::OrderCanceled
///         ↓
///     Canceled
///
/// Fills may still arrive while cancellation is in flight. The adapter
/// event loop therefore remains free to process `UserEvents::Fills` and
/// `OrderUpdates` while this request is awaiting Hyperliquid.
///
/// The Engine remains authoritative for filled quantity and final state.
pub async fn handle_cancel_order(
    exchange_client: &ExchangeClient,
    tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    order_ref: HyperliquidOrderRef,
    debug_mode: bool,
) {
    // ============================================================
    // ORDER REFERENCE
    // ============================================================

    // Correlation has already been resolved synchronously by adapter.rs
    // before this task was spawned.
    //
    // The task owns this lightweight transport reference and therefore
    // does not borrow adapter-local correlation across an await.
    let client_order_id =
        order_ref.client_order_id;

    let symbol =
        order_ref.symbol;


    // ============================================================
    // CLOID
    // ============================================================

    // The Engine-generated ClientOrderId was originally submitted to
    // Hyperliquid as CLOID.
    //
    // Reconstruct the same lossless venue representation so the existing
    // order can be addressed without depending on Hyperliquid's numeric OID.
    let cloid =
        to_hyperliquid_cloid(
            client_order_id
        );


    // ============================================================
    // REQUEST
    // ============================================================

    let request =
        ClientCancelRequestCloid {
            asset:
                symbol.clone(),

            cloid,
        };


    // ============================================================
    // SUBMIT CANCEL
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID ADAPTER] CANCEL SUBMIT | order_id={} | client_order_id={} | symbol={}",
            order_id,
            client_order_id,
            symbol,
        ),
    );


    let raw_response =
        match exchange_client
            .cancel_by_cloid(
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
                        "[HYPERLIQUID ADAPTER] CANCEL ERROR | order_id={} | symbol={} | error={:?}",
                        order_id,
                        symbol,
                        error,
                    ),
                );


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id:
                                Some(order_id),

                            message:
                                format!(
                                    "Hyperliquid cancel request failed: {:?}",
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
                        "[HYPERLIQUID ADAPTER] CANCEL REJECTED | order_id={} | symbol={} | error={:?}",
                        order_id,
                        symbol,
                        error,
                    ),
                );


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id:
                                Some(order_id),

                            message:
                                format!(
                                    "Hyperliquid cancel rejected: {:?}",
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
                "[HYPERLIQUID ADAPTER] EMPTY CANCEL RESPONSE | order_id={} | symbol={}",
                order_id,
                symbol,
            ),
        );


        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id:
                        Some(order_id),

                    message:
                        "Hyperliquid cancel response contained no data"
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
                "[HYPERLIQUID ADAPTER] CANCEL RESPONSE HAS NO STATUS | order_id={} | symbol={}",
                order_id,
                symbol,
            ),
        );


        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id:
                        Some(order_id),

                    message:
                        "Hyperliquid cancel response contained no status"
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
        // SUCCESS
        // ========================================================

        ExchangeDataStatus::Success => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] CANCEL ACCEPTED | order_id={} | client_order_id={} | symbol={} | confirmation_source=OrderUpdates",
                    order_id,
                    client_order_id,
                    symbol,
                ),
            );


            // Do NOT emit OrderCanceled here.
            //
            // The synchronous response confirms only that Hyperliquid
            // accepted the cancellation request.
            //
            // The order may have received additional executions while the
            // cancellation request was in flight.
            //
            // OrderUpdates "canceled" provides the authoritative lifecycle
            // confirmation:
            //
            //     Hyperliquid OrderUpdates
            //         ↓
            //     status = "canceled"
            //         ↓
            //     mapper/order_update.rs
            //         ↓
            //     ExecEvent::OrderCanceled
            //         ↓
            //     Engine
            //         ↓
            //     OrderState::Canceled
        }


        // ========================================================
        // ERROR
        // ========================================================

        ExchangeDataStatus::Error(error) => {
            let ts_ms =
                Utc::now()
                    .timestamp_millis();


            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] CANCEL REJECTED | order_id={} | symbol={} | error={}",
                    order_id,
                    symbol,
                    error,
                ),
            );


            let _ =
                tx_evt.send(
                    ExecEvent::Error {
                        order_id:
                            Some(order_id),

                        message:
                            format!(
                                "Hyperliquid cancel rejected: {}",
                                error,
                            ),

                        ts_ms,
                    }
                );
        }


        // ========================================================
        // OTHER
        // ========================================================

        other => {
            let ts_ms =
                Utc::now()
                    .timestamp_millis();


            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID ADAPTER] UNHANDLED CANCEL STATUS | order_id={} | symbol={} | status={:?}",
                    order_id,
                    symbol,
                    other,
                ),
            );


            let _ =
                tx_evt.send(
                    ExecEvent::Error {
                        order_id:
                            Some(order_id),

                        message:
                            format!(
                                "Unhandled Hyperliquid cancel status: {:?}",
                                other,
                            ),

                        ts_ms,
                    }
                );
        }
    }
}