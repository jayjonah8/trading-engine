#![allow(dead_code)]

use crate::exchange::hyperliquid::api::update_leverage::update_leverage;


// ============================================================
// TEST HYPERLIQUID UPDATE LEVERAGE
// ============================================================

// IMPORTANT:
// This changes the leverage configuration for the configured Hyperliquid
// account on Mainnet. false selects isolated margin; true selects cross margin.
//
// Run:
// cargo test test_hyperliquid_update_leverage -- --ignored --nocapture

#[tokio::test]
#[ignore = "requires Hyperliquid private key and changes account leverage configuration"]
async fn test_hyperliquid_update_leverage() {
    dotenvy::dotenv().ok();

    let symbol = "BTC";
    let leverage = 5;
    let is_cross = false;

    update_leverage(leverage, symbol, is_cross).await;
}