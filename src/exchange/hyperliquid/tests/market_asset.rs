#![allow(dead_code)]

use crate::exchange::hyperliquid::api::get_market_asset;


// ============================================================
// TEST HYPERLIQUID MARKET ASSET
// ============================================================

// Fetches current Hyperliquid market information for a single asset.
//
// Run:
// cargo test test_single_hyperliquid_market_asset -- --nocapture

#[tokio::test]
async fn test_single_hyperliquid_market_asset() {
    let symbol = "BTC";

    let asset = get_market_asset(symbol)
        .await
        .expect("Failed to fetch Hyperliquid market asset");

    println!("\n========================================");
    println!("[HYPERLIQUID TEST] MARKET ASSET");
    println!("========================================");
    println!("Symbol:        {}", asset.symbol);
    println!("24h Volume:    ${:.2}", asset.volume);
    println!("Max Leverage:  {}x", asset.max_leverage);
    println!("24h PnL:       {:.2}%", asset.pnl_24h_pct);
    println!("Open Interest: ${:.2}", asset.open_interest);
    println!("Funding:       {}", asset.funding);
    println!("Impact Bid:    {:.4}", asset.impact_pxs.0);
    println!("Impact Ask:    {:.4}", asset.impact_pxs.1);
    println!("========================================");
}

// Example Output:
// ========================================
// [HYPERLIQUID TEST] MARKET ASSET
// ========================================
// Symbol:        BTC
// 24h Volume:    $2595087759.60
// Max Leverage:  40x
// 24h PnL:       -1.91%
// Open Interest: $3107886195.97
// Funding:       -0.0000010029
// Impact Bid:    84555.0000
// Impact Ask:    84556.0000
// ========================================