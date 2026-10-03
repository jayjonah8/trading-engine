#![allow(dead_code)]

use crate::exchange::hyperliquid::api::get_market_assets;


// ============================================================
// TEST HYPERLIQUID MARKET ASSETS
// ============================================================

// Fetches current Hyperliquid market information and prints every active
// asset ordered from highest to lowest 24h notional volume.
//
// Run:
// cargo test test_hyperliquid_market_assets -- --nocapture

#[tokio::test]
async fn test_hyperliquid_market_assets() {
    let assets = get_market_assets()
        .await
        .expect("Failed to fetch Hyperliquid market assets");

    assert!(!assets.is_empty(), "Hyperliquid returned no market assets");

    println!("\n========================================");
    println!("[HYPERLIQUID TEST] MARKET ASSETS");
    println!("Assets: {}", assets.len());
    println!("========================================");

    for asset in &assets {
        println!(
            "{} | volume=${:.2} | leverage={}x | pnl24h={:.2}% | oi=${:.2} | funding={} | impact=({:.4}, {:.4})",
            asset.symbol,
            asset.volume,
            asset.max_leverage,
            asset.pnl_24h_pct,
            asset.open_interest,
            asset.funding,
            asset.impact_pxs.0,
            asset.impact_pxs.1,
        );
    }

    println!("========================================");
}

// Example Output:
// ========================================
// [HYPERLIQUID TEST] MARKET ASSETS
// Assets: 178
// ========================================
// BTC | volume=$2593769557.25 | leverage=40x | pnl24h=-1.90% | oi=$3109605893.44 | funding=-0.0000024243 | impact=(84568.0000, 84569.0000)
// ETH | volume=$1161898224.71 | leverage=25x | pnl24h=-2.30% | oi=$3229730968.67 | funding=0.0000125 | impact=(2684.1000, 2684.2000)
// HYPE | volume=$564081020.03 | leverage=10x | pnl24h=-2.48% | oi=$1773153590.26 | funding=0.0000125 | impact=(87.8400, 87.8410)
// ZEC | volume=$324771545.79 | leverage=10x | pnl24h=-5.37% | oi=$559389238.48 | funding=0.0000125 | impact=(1313.9000, 1314.0348)
// SOL | volume=$237257874.05 | leverage=20x | pnl24h=-2.14% | oi=$652839200.82 | funding=-0.0000088755 | impact=(119.1800, 119.1900)
// PUMP | volume=$212978573.90 | leverage=10x | pnl24h=-6.30% | oi=$266595995.37 | funding=0.0000125 | impact=(0.0055, 0.0055)
// NEAR | volume=$151685237.56 | leverage=10x | pnl24h=-5.36% | oi=$323411567.79 | funding=0.0000125 | impact=(4.6505, 4.6522)
// XRP | volume=$139218339.82 | leverage=20x | pnl24h=-3.57% | oi=$288208848.20 | funding=0.0000125 | impact=(1.4823, 1.4824)
// SAND | volume=$78885384.89 | leverage=5x | pnl24h=17.46% | oi=$5780568.62 | funding=-0.0001481396 | impact=(0.0734, 0.0734)
// ...