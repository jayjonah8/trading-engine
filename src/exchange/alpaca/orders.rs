#![allow(dead_code)]

use crate::exchange::alpaca::types::AlpacaConfig;


// ============================================================
// PLACE MARKET ORDER
// ============================================================

/// Places an Alpaca market order using notional position sizing.
///
/// `cid` is the engine-generated client order identifier sent to Alpaca as
/// `client_order_id`.
///
/// Alpaca returns its own venue order ID. The execution adapter uses that
/// broker order ID to correlate later `trade_updates` WebSocket events with
/// the canonical engine order that caused the submission.
///
/// This function is responsible only for communicating with Alpaca's REST API.
/// Canonical order, trade, fill, position, and risk state remain owned by the
/// engine.
///
/// TODO:
/// Replace this market-order-specific interface with a generic Alpaca order
/// submission path once the new canonical engine order model is fully wired.
/// The future path should support market and limit orders, quantity or notional
/// sizing, limit price, time in force, and other canonical order parameters.
/// This module should remain responsible only for translating canonical engine
/// orders into Alpaca REST requests.
pub(crate) async fn place_market_order(
    config: &AlpacaConfig,
    cid: &str,
    symbol: &str,
    side: &str,
    notional: f64,
) -> anyhow::Result<String> {

    // ============================================================
    // BUILD REQUEST
    // ============================================================

    let client = reqwest::Client::new();

    let body =
        serde_json::json!({
            "symbol": symbol,
            "notional": notional.to_string(),
            "side": side,
            "type": "market",
            "time_in_force": "day",
            "client_order_id": cid,
        });


    // ============================================================
    // SEND ORDER
    // ============================================================

    let response =
        client
            .post(
                format!(
                    "{}/v2/orders",
                    config.base_url
                )
            )
            .header(
                "APCA-API-KEY-ID",
                &config.api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                &config.api_secret,
            )
            .json(
                &body
            )
            .send()
            .await?;


    let status = response.status();

    let body: serde_json::Value =
        response
            .json()
            .await?;


    // ============================================================
    // VALIDATE RESPONSE
    // ============================================================

    if !status.is_success() {
        anyhow::bail!(
            "Alpaca order failed status={} body={}",
            status,
            body
        );
    }


    let order_id =
        body
            .get("id")
            .and_then(
                |value| {
                    value.as_str()
                }
            )
            .ok_or_else(
                || {
                    anyhow::anyhow!(
                        "Alpaca order response missing id: {}",
                        body
                    )
                }
            )?
            .to_string();


    Ok(order_id)
}


// ============================================================
// CLOSE POSITION
// ============================================================

/// Requests that Alpaca reduce or fully close an existing broker position.
///
/// `None` requests closure of the entire Alpaca position for `symbol`.
///
/// `Some(qty)` requests a partial close for the specified quantity.
///
/// Alpaca returns the venue order ID generated for the close request. That ID
/// can then be correlated with later `trade_updates` execution messages.
///
/// This function does not mutate canonical engine state. The engine updates its
/// order, trade, position, and fill state only after execution events are
/// received from the Alpaca adapter.
///
/// TODO:
/// Revisit this broker-specific close-position path once the new order-centric
/// engine execution flow is complete. Prefer representing exits as canonical
/// engine exit orders so entries and exits follow the same Order -> ExecCommand
/// -> venue -> ExecEvent lifecycle. Keep Alpaca's direct position-close endpoint
/// only where it provides a useful venue-specific capability.
pub(crate) async fn close_position(
    config: &AlpacaConfig,
    symbol: &str,
    qty: Option<f64>,
) -> anyhow::Result<String> {

    // ============================================================
    // BUILD REQUEST
    // ============================================================

    let client = reqwest::Client::new();

    let url =
        format!(
            "{}/v2/positions/{}",
            config.base_url,
            symbol
        );

    let mut request =
        client
            .delete(
                &url
            )
            .header(
                "APCA-API-KEY-ID",
                &config.api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                &config.api_secret,
            );


    // None closes the entire Alpaca position.
    //
    // Some(qty) requests a partial close.
    if let Some(qty) = qty {
        request =
            request.query(
                &[
                    (
                        "qty",
                        qty.to_string()
                    )
                ]
            );
    }


    // ============================================================
    // SEND CLOSE
    // ============================================================

    let response =
        request
            .send()
            .await?;


    let status = response.status();

    let body: serde_json::Value =
        response
            .json()
            .await?;


    // ============================================================
    // VALIDATE RESPONSE
    // ============================================================

    if !status.is_success() {
        anyhow::bail!(
            "Alpaca close failed status={} body={}",
            status,
            body
        );
    }


    let order_id =
        body
            .get("id")
            .and_then(
                |value| {
                    value.as_str()
                }
            )
            .ok_or_else(
                || {
                    anyhow::anyhow!(
                        "Alpaca close response missing id: {}",
                        body
                    )
                }
            )?
            .to_string();


    Ok(order_id)
}