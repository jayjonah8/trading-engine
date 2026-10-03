#![allow(dead_code)]

use crate::exchange::alpaca::{
    types::AlpacaAsset,
    utils::get_alpaca_config,
};


// ============================================================
// GET ALPACA ASSET
// ============================================================

/// Fetches Alpaca's current metadata for one asset.
///
/// `symbol_or_asset_id` may be an Alpaca symbol such as `AAPL` or an
/// Alpaca asset ID.
///
/// The active paper or live environment is selected through
/// `get_alpaca_config()`, ensuring this request uses the same credentials
/// and REST endpoint as the production execution adapter.
///
/// Returns an error if configuration cannot be loaded, the Alpaca request
/// fails, Alpaca returns a non-success HTTP status, or the response cannot
/// be parsed.
pub async fn get_alpaca_asset(
    symbol_or_asset_id: &str,
) -> anyhow::Result<AlpacaAsset> {
    let config = get_alpaca_config()?;


    // ============================================================
    // REQUEST ASSET
    // ============================================================

    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/v2/assets/{}", config.base_url, symbol_or_asset_id))
        .header("APCA-API-KEY-ID", &config.api_key)
        .header("APCA-API-SECRET-KEY", &config.api_secret)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;


    // ============================================================
    // VALIDATE RESPONSE
    // ============================================================

    if !status.is_success() {
        anyhow::bail!(
            "Alpaca asset request failed | symbol_or_asset_id={} | status={} | body={}",
            symbol_or_asset_id,
            status,
            body
        );
    }


    // ============================================================
    // PARSE ASSET
    // ============================================================

    let asset = serde_json::from_str::<AlpacaAsset>(&body)?;

    Ok(asset)
}