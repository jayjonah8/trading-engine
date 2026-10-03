#![allow(dead_code)]

use crate::exchange::hyperliquid::{
    api::market_assets::get_market_assets,
    types::MarketAsset,
};


// ============================================================
// GET HYPERLIQUID MARKET ASSET
// ============================================================

// Fetches current Hyperliquid market information for one symbol.
//
// The symbol should use Hyperliquid's native symbol format, for example:
// "BTC", "ETH", or "SOL".
//
// Returns an error if the symbol is not present in the active market universe.
pub async fn get_market_asset(symbol: &str) -> anyhow::Result<MarketAsset> {
    let assets = get_market_assets().await?;

    assets
        .into_iter()
        .find(|asset| asset.symbol.eq_ignore_ascii_case(symbol))
        .ok_or_else(|| anyhow::anyhow!("Hyperliquid market asset not found | symbol={}", symbol))
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