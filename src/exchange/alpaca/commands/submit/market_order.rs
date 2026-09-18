#![allow(dead_code)]

use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        Order,
        Side,
        TimeInForce,
    },

    exchange::alpaca::{
        submission_result::AlpacaSubmissionResult,
        types::AlpacaConfig,
    },

    shared::print_log,
};


// ============================================================
// HANDLE MARKET ORDER
// ============================================================

/// Submits a canonical market order to Alpaca.
///
/// Both entry and exit orders use Alpaca's normal `/v2/orders` endpoint.
///
/// Execution behavior is represented by the canonical:
///
///     Side + quantity
///
/// rather than by separate venue-specific entry/exit paths.
///
/// The immediate REST response is used for two things:
///
/// - obtaining Alpaca's venue order ID
/// - reporting canonical `ExecEvent::OrderAccepted` to the Engine
///
/// The venue order ID is also sent back to the authoritative adapter event
/// loop through `AlpacaSubmissionResult`.
///
/// This allows the adapter to update `AlpacaOrderCorrelation` without giving
/// spawned REST tasks shared mutable access to correlation state.
///
/// Actual executions do NOT come from this REST response. They arrive later
/// through Alpaca `trade_updates` and are translated into canonical
/// `ExecEvent::Fill` events.
pub async fn handle_market_order(
    alpaca_config: &AlpacaConfig,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<AlpacaSubmissionResult>,
    order: Order,
    debug_mode: bool,
) {
    // ============================================================
    // SIDE
    // ============================================================

    let side = match order.side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    };


    // ============================================================
    // TIME IN FORCE
    // ============================================================

    let tif = match order.time_in_force {
        TimeInForce::Day => "day",
        TimeInForce::Gtc => "gtc",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
    };


    // ============================================================
    // VALIDATION
    // ============================================================

    if order.post_only {
        let ts_ms = Utc::now().timestamp_millis();

        let _ = tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order.id),
                message: "Alpaca market orders cannot be post_only".to_string(),
                ts_ms,
            }
        );

        return;
    }


    // ============================================================
    // REQUEST
    // ============================================================

    let body = serde_json::json!({
        "symbol": order.symbol,
        "qty": order.quantity.to_string(),
        "side": side,
        "type": "market",
        "time_in_force": tif,
        "client_order_id": order.client_order_id.to_string(),
    });


    // ============================================================
    // SUBMIT
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] MARKET SUBMIT | order_id={} | client_order_id={} | purpose={:?} | symbol={} | side={:?} | qty={} | tif={} | reduce_only={}",
            order.id,
            order.client_order_id,
            order.purpose,
            order.symbol,
            order.side,
            order.quantity,
            tif,
            order.reduce_only,
        ),
    );


    let client = reqwest::Client::new();


    let response = match client
        .post(format!("{}/v2/orders", alpaca_config.base_url))
        .header("APCA-API-KEY-ID", &alpaca_config.api_key)
        .header("APCA-API-SECRET-KEY", &alpaca_config.api_secret)
        .json(&body)
        .send()
        .await
    {
        Ok(response) => response,

        Err(error) => {
            let ts_ms = Utc::now().timestamp_millis();

            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA ADAPTER] MARKET SUBMIT ERROR | order_id={} | symbol={} | error={:?}",
                    order.id,
                    order.symbol,
                    error,
                ),
            );

            let _ = tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message: format!("Alpaca market order submission failed: {:?}", error),
                    ts_ms,
                }
            );

            return;
        }
    };


    // ============================================================
    // RESPONSE
    // ============================================================

    let status = response.status();


    let body: Value = match response.json().await {
        Ok(body) => body,

        Err(error) => {
            let ts_ms = Utc::now().timestamp_millis();

            print_log(
                debug_mode,
                true,
                format!(
                    "[ALPACA ADAPTER] MARKET RESPONSE DECODE ERROR | order_id={} | symbol={} | error={:?}",
                    order.id,
                    order.symbol,
                    error,
                ),
            );

            let _ = tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message: format!("Failed to decode Alpaca market order response: {:?}", error),
                    ts_ms,
                }
            );

            return;
        }
    };


    // ============================================================
    // REJECTED
    // ============================================================

    if !status.is_success() {
        let ts_ms = Utc::now().timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] MARKET REJECTED | order_id={} | symbol={} | status={} | body={}",
                order.id,
                order.symbol,
                status,
                body,
            ),
        );

        let _ = tx_evt.send(
            ExecEvent::OrderRejected {
                order_id: order.id,
                reason: format!("status={} body={}", status, body),
                ts_ms,
            }
        );

        return;
    }


    // ============================================================
    // VENUE ORDER ID
    // ============================================================

    let Some(venue_order_id) = body
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        let ts_ms = Utc::now().timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] MARKET RESPONSE MISSING ORDER ID | order_id={} | symbol={} | body={}",
                order.id,
                order.symbol,
                body,
            ),
        );

        let _ = tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order.id),
                message: format!("Alpaca market response missing order id: {}", body),
                ts_ms,
            }
        );

        return;
    };


    // ============================================================
    // RETURN VENUE ORDER ID TO ADAPTER
    // ============================================================

    // The REST task must not mutate AlpacaOrderCorrelation directly.
    //
    // Instead, send the newly assigned Alpaca venue order ID back to the
    // adapter event loop. The adapter remains the single authoritative owner
    // of correlation state and will call:
    //
    //     correlation.set_venue_order_id(...)
    //
    // This avoids Arc<Mutex<AlpacaOrderCorrelation>> while still making the
    // venue order ID available for later cancel/replace operations.
    if tx_submission_result.send(
        AlpacaSubmissionResult {
            order_id: order.id,
            venue_order_id: venue_order_id.clone(),
        }
    ).is_err() {
        let ts_ms = Utc::now().timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] MARKET CORRELATION RESULT FAILED | order_id={} | venue_order_id={}",
                order.id,
                venue_order_id,
            ),
        );

        let _ = tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order.id),
                message: "Alpaca submission result channel closed".to_string(),
                ts_ms,
            }
        );

        return;
    }


    // ============================================================
    // ACCEPTED
    // ============================================================

    // At this point:
    //
    // - Alpaca accepted the REST submission
    // - Alpaca assigned a venue order ID
    // - that venue order ID has been queued back to the adapter for
    //   correlation
    //
    // REST acceptance does NOT prove that the order filled or is currently
    // working. Those facts remain authoritative from `trade_updates`.
    let ts_ms = Utc::now().timestamp_millis();

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] MARKET ACCEPTED | order_id={} | symbol={} | venue_order_id={}",
            order.id,
            order.symbol,
            venue_order_id,
        ),
    );

    let _ = tx_evt.send(
        ExecEvent::OrderAccepted {
            order_id: order.id,
            venue_order_id,
            ts_ms,
        }
    );
}