use crate::exchange::alpaca_utils::{
    AlpacaConfig,
};


/// Places an Alpaca market order using notional position sizing.
///
/// The returned Alpaca order ID is used by the execution adapter to correlate
/// future trade_updates WebSocket events with the engine's original command.
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

    let client =
        reqwest::Client::new();


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


    let status =
        response.status();


    let body:
        serde_json::Value =
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
            .get(
                "id"
            )
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


    Ok(
        order_id
    )
}


// ============================================================
// ALPACA CLOSE POSITION
// ============================================================

/// Requests that Alpaca close an existing position.
///
/// `None` closes the entire broker position for the symbol while `Some(qty)`
/// requests a partial close for the specified quantity.
///
/// The returned Alpaca order ID is stored by the execution adapter so later
/// trade_updates can be correlated with the engine's PendingClose.
pub(crate) async fn close_position(
    config: &AlpacaConfig,
    symbol: &str,
    qty: Option<f64>,
) -> anyhow::Result<String> {

    // ============================================================
    // BUILD REQUEST
    // ============================================================

    let client =
        reqwest::Client::new();


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
    // Some(qty) requests a partial close for that quantity.
    if let Some(qty) =
        qty
    {
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


    let status =
        response.status();


    let body:
        serde_json::Value =
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
            .get(
                "id"
            )
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


    Ok(
        order_id
    )
}