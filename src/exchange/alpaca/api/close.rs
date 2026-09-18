// exchange/alpaca/api/close_position.rs

#![allow(dead_code)]

use crate::exchange::alpaca::types::AlpacaConfig;


// ============================================================
// CLOSE POSITION
// ============================================================

/// Directly requests that Alpaca reduce or fully close an existing
/// venue position.
///
/// This is a venue-specific Alpaca API operation and is NOT the normal
/// canonical Engine trade-exit path.
///
/// Normal strategy exits should use:
///
///     Order
///       -> ExecCommand::SubmitOrder
///       -> Alpaca adapter
///       -> /v2/orders
///       -> trade_updates
///       -> ExecEvent
///
/// This direct endpoint is retained for venue-level operations such as:
///
/// - emergency flattening
/// - startup/reconciliation tooling
/// - administrative position management
///
/// `None` closes the entire Alpaca position.
///
/// `Some(qty)` requests a partial close for the specified quantity.
///
/// Alpaca returns the venue order ID generated for the close request.
///
/// This function never mutates canonical Engine Order, Trade, Position,
/// FillLedger, or Risk state.
pub async fn close_position(
    config: &AlpacaConfig,
    symbol: &str,
    qty: Option<f64>,
) -> anyhow::Result<String> {
    // ============================================================
    // BUILD REQUEST
    // ============================================================

    let client = reqwest::Client::new();
    let url = format!("{}/v2/positions/{}", config.base_url, symbol);

    let mut request = client
        .delete(&url)
        .header("APCA-API-KEY-ID", &config.api_key)
        .header("APCA-API-SECRET-KEY", &config.api_secret);

    // None closes the entire Alpaca position.
    //
    // Some(qty) requests a partial close.
    if let Some(qty) = qty {
        request = request.query(&[("qty", qty.to_string())]);
    }


    // ============================================================
    // SEND CLOSE
    // ============================================================

    let response = request.send().await?;
    let status = response.status();
    let body: serde_json::Value = response.json().await?;


    // ============================================================
    // VALIDATE RESPONSE
    // ============================================================

    if !status.is_success() {
        anyhow::bail!("Alpaca close failed status={} body={}", status, body);
    }

    let order_id = body
        .get("id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("Alpaca close response missing id: {}", body))?
        .to_string();

    Ok(order_id)
}