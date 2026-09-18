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
// HANDLE LIMIT ORDER
// ============================================================

/// Submits a canonical limit order to Alpaca.
///
/// Both entry and exit limit orders use Alpaca's normal `/v2/orders` path.
///
/// Execution behavior is represented by:
///
///     Side + quantity + limit_px
///
/// rather than separate venue-specific entry/exit paths.
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
/// Actual executions and subsequent lifecycle changes do NOT come from this
/// REST response. They arrive through Alpaca `trade_updates` and are
/// translated separately into canonical `ExecEvent`s.
///
/// Canonical Engine state is never mutated here.
pub async fn handle_limit_order(
    alpaca_config: &AlpacaConfig,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<AlpacaSubmissionResult>,
    order: Order,
    debug_mode: bool,
) {
    // ============================================================
    // LIMIT PRICE
    // ============================================================

    let Some(limit_px) = order.limit_px else {
        let ts_ms = Utc::now().timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] LIMIT ORDER MISSING PRICE | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );

        let _ = tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order.id),
                message: "Alpaca limit order missing limit_px".to_string(),
                ts_ms,
            }
        );

        return;
    };


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
        TimeInForce::Gtc => "gtc",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
        TimeInForce::Day => "day",
    };


    // ============================================================
    // POST ONLY
    // ============================================================

    // Alpaca's normal equity order API does not expose the same generic
    // post-only execution instruction used by venues such as Hyperliquid.
    //
    // Do not silently ignore canonical execution semantics. Reject the
    // request until an explicitly supported Alpaca mapping is implemented.
    if order.post_only {
        let ts_ms = Utc::now().timestamp_millis();

        print_log(
            debug_mode,
            true,
            format!(
                "[ALPACA ADAPTER] LIMIT POST ONLY NOT SUPPORTED | order_id={} | symbol={}",
                order.id,
                order.symbol,
            ),
        );

        let _ = tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order.id),
                message: "Alpaca post_only limit orders are not implemented".to_string(),
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
        "type": "limit",
        "time_in_force": tif,
        "limit_price": limit_px.to_string(),
        "client_order_id": order.client_order_id.to_string(),
    });


    // ============================================================
    // SUBMIT
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] LIMIT SUBMIT | order_id={} | client_order_id={} | purpose={:?} | symbol={} | side={:?} | qty={} | limit_px={} | tif={} | reduce_only={} | post_only={}",
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
                    "[ALPACA ADAPTER] LIMIT SUBMIT ERROR | order_id={} | symbol={} | error={:?}",
                    order.id,
                    order.symbol,
                    error,
                ),
            );

            let _ = tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message: format!("Alpaca limit order submission failed: {:?}", error),
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
                    "[ALPACA ADAPTER] LIMIT RESPONSE DECODE ERROR | order_id={} | symbol={} | error={:?}",
                    order.id,
                    order.symbol,
                    error,
                ),
            );

            let _ = tx_evt.send(
                ExecEvent::Error {
                    order_id: Some(order.id),
                    message: format!("Failed to decode Alpaca limit order response: {:?}", error),
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
                "[ALPACA ADAPTER] LIMIT REJECTED | order_id={} | symbol={} | status={} | body={}",
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
                "[ALPACA ADAPTER] LIMIT RESPONSE MISSING ORDER ID | order_id={} | symbol={} | body={}",
                order.id,
                order.symbol,
                body,
            ),
        );

        let _ = tx_evt.send(
            ExecEvent::Error {
                order_id: Some(order.id),
                message: format!("Alpaca limit response missing order id: {}", body),
                ts_ms,
            }
        );

        return;
    };


    // ============================================================
    // RETURN VENUE ORDER ID TO ADAPTER
    // ============================================================

    // The spawned REST task does not own or mutate AlpacaOrderCorrelation.
    //
    // Instead, Alpaca's assigned venue order ID is returned to the
    // authoritative adapter event loop through this private channel.
    //
    // The adapter will then perform:
    //
    //     correlation.set_venue_order_id(...)
    //
    // This preserves single ownership of correlation state while making the
    // venue order ID available for future cancel/replace operations.
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
                "[ALPACA ADAPTER] LIMIT CORRELATION RESULT FAILED | order_id={} | venue_order_id={}",
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
    // working. Alpaca `trade_updates` remains authoritative for those facts.
    let ts_ms = Utc::now().timestamp_millis();

    print_log(
        debug_mode,
        true,
        format!(
            "[ALPACA ADAPTER] LIMIT ACCEPTED | order_id={} | symbol={} | venue_order_id={} | limit_px={}",
            order.id,
            order.symbol,
            venue_order_id,
            limit_px,
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