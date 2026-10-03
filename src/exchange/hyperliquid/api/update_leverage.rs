#![allow(dead_code)]

use std::env;

use hyperliquid_rust_sdk::{
    BaseUrl,
    ExchangeClient,
    ExchangeResponseStatus,
};


// ============================================================
// UPDATE HYPERLIQUID LEVERAGE
// ============================================================

// Updates the leverage configuration for a Hyperliquid symbol.
//
// leverage:
//     Desired leverage multiplier. Example: 10 = 10x.
//
// symbol:
//     Hyperliquid coin/symbol. Example: "BTC".
//
// is_cross:
//     true  = cross margin
//     false = isolated margin
//
// Hyperliquid requires the margin mode to be supplied when leverage is
// updated. The trading engine currently uses isolated margin, so callers
// should normally pass false.
pub async fn update_leverage(leverage: u32, symbol: &str, is_cross: bool) {
    let private_key = env::var("HYPERLIQUID_PRIVATE_KEY")
        .expect("Missing HYPERLIQUID_PRIVATE_KEY env var");

    let wallet = private_key
        .parse()
        .expect("Invalid HYPERLIQUID_PRIVATE_KEY in update_leverage()");

    let exchange_client = ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None)
        .await
        .expect("Failed to create Hyperliquid ExchangeClient");

    let response = exchange_client
        .update_leverage(leverage, symbol, is_cross, None)
        .await
        .expect("Failed to send Hyperliquid update leverage request");

    match response {
        ExchangeResponseStatus::Ok(_) => {
            println!(
                "[HYPERLIQUID] LEVERAGE UPDATED | symbol={} | leverage={}x | is_cross={}",
                symbol, leverage, is_cross
            );
        }
        ExchangeResponseStatus::Err(error) => {
            eprintln!(
                "[HYPERLIQUID] LEVERAGE UPDATE FAILED | symbol={} | leverage={}x | is_cross={} | error={:?}",
                symbol, leverage, is_cross, error
            );
        }
    }
}