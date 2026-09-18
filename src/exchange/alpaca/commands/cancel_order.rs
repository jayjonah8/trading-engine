#![allow(dead_code)]

use chrono::Utc;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        OrderId,
    },

    exchange::alpaca::{
        correlation::AlpacaOrderRef,
        types::AlpacaConfig,
    },

    shared::print_log,
};


// ============================================================
// HANDLE CANCEL ORDER
// ============================================================

/// Requests cancellation of an existing Alpaca order.
///
/// The adapter resolves the canonical `OrderId` into an owned
/// `AlpacaOrderRef` BEFORE spawning this asynchronous request.
///
/// Resolution occurs in `adapter.rs`:
///
///     OrderId
///         ↓
///     AlpacaOrderCorrelation
///         ↓
///     AlpacaOrderRef
///         ├── ClientOrderId
///         └── venue_order_id
///         ↓
///     spawned cancellation task
///         ↓
///     DELETE /v2/orders/{venue_order_id}
///
/// This keeps `AlpacaOrderCorrelation` owned exclusively by the main adapter
/// event loop. The spawned cancellation task receives only the lightweight
/// transport information required by Alpaca.
///
/// IMPORTANT:
///
/// A successful REST cancellation response means Alpaca accepted the
/// cancellation request.
///
/// It does NOT directly emit `ExecEvent::OrderCanceled`.
///
/// Authoritative asynchronous cancellation confirmation comes from:
///
///     trade_updates
///         ↓
///     event = "canceled"
///         ↓
///     Alpaca mapper
///         ↓
///     ExecEvent::OrderCanceled
///         ↓
///     Engine
///
/// Fills may still arrive while cancellation is in flight, so the adapter
/// remains free to process `trade_updates` while this REST request awaits.
pub async fn handle_cancel_order(
    alpaca_config: &AlpacaConfig,
    tx_evt: &UnboundedSender<ExecEvent>,
    order_id: OrderId,
    order_ref: AlpacaOrderRef,
    debug_mode: bool,
) {
    // ============================================================
    // ORDER REFERENCE
    // ============================================================

    let client_order_id =
        order_ref.client_order_id;


    let Some(venue_order_id) =
        order_ref.venue_order_id
    else {
        let ts_ms =
            Utc::now()
                .timestamp_millis();


        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] CANCEL VENUE ORDER ID NOT AVAILABLE | order_id={} | client_order_id={}",
                order_id,
                client_order_id,
            ),
        );


        let _ =
            tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order_id),
                    message: format!(
                        "Alpaca cancellation cannot resolve venue_order_id for order_id={}",
                        order_id,
                    ),
                    ts_ms,
                }
            );


        return;
    };


    // ============================================================
    // SUBMIT CANCEL
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] CANCEL SUBMIT | order_id={} | client_order_id={} | venue_order_id={}",
            order_id,
            client_order_id,
            venue_order_id,
        ),
    );


    let client =
        reqwest::Client::new();


    let response =
        match client
            .delete(
                format!(
                    "{}/v2/orders/{}",
                    alpaca_config.base_url,
                    venue_order_id,
                )
            )
            .header(
                "APCA-API-KEY-ID",
                &alpaca_config.api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                &alpaca_config.api_secret,
            )
            .send()
            .await
        {
            Ok(response) => response,

            Err(error) => {
                let ts_ms =
                    Utc::now()
                        .timestamp_millis();


                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[ALPACA ADAPTER] CANCEL ERROR | order_id={} | venue_order_id={} | error={:?}",
                        order_id,
                        venue_order_id,
                        error,
                    ),
                );


                let _ =
                    tx_evt.send(
                        ExecEvent::Error {
                            order_id: Some(order_id),
                            message: format!(
                                "Alpaca cancel request failed: {:?}",
                                error,
                            ),
                            ts_ms,
                        }
                    );


                return;
            }
        };


    // ============================================================
    // RESPONSE
    // ============================================================

    let status =
        response.status();


    // Alpaca cancellation success does not need a JSON body.
    //
    // A successful response means the REST cancellation request was accepted.
    // Final canonical cancellation still comes from trade_updates.
    if status.is_success() {
        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] CANCEL ACCEPTED | order_id={} | client_order_id={} | venue_order_id={} | confirmation_source=trade_updates",
                order_id,
                client_order_id,
                venue_order_id,
            ),
        );


        return;
    }


    // ============================================================
    // REJECTED
    // ============================================================

    let body =
        response
            .text()
            .await
            .unwrap_or_else(
                |_| {
                    "<failed to read Alpaca response body>"
                        .to_string()
                }
            );


    let ts_ms =
        Utc::now()
            .timestamp_millis();


    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] CANCEL REJECTED | order_id={} | venue_order_id={} | status={} | body={}",
            order_id,
            venue_order_id,
            status,
            body,
        ),
    );


    let _ =
        tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order_id),
                message: format!(
                    "Alpaca cancel rejected | status={} | body={}",
                    status,
                    body,
                ),
                ts_ms,
            }
        );
}