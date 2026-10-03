#![allow(dead_code)]

use crate::exchange::hyperliquid::types::{MarketAsset, MetaAndCtxs};


// ============================================================
// HELPERS
// ============================================================

fn parse_num(value: Option<&str>) -> f64 {
    value.unwrap_or("0").parse::<f64>().unwrap_or(0.0)
}


// ============================================================
// GET HYPERLIQUID MARKET ASSETS
// ============================================================

// Fetches current Hyperliquid perpetual market information for all assets.
//
// Returns one MarketAsset per active asset containing:
// - 24h notional volume
// - maximum leverage
// - 24h price change
// - USD open interest
// - current funding
// - impact bid / ask prices
//
// Assets are returned highest 24h volume first.
pub async fn get_market_assets() -> anyhow::Result<Vec<MarketAsset>> {
    #[derive(serde::Serialize)]
    struct RequestBody<'a> {
        r#type: &'a str,
    }

    let response = reqwest::Client::new()
        .post("https://api.hyperliquid.xyz/info")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&RequestBody { r#type: "metaAndAssetCtxs" })
        .send()
        .await?
        .error_for_status()?
        .json::<MetaAndCtxs>()
        .await?;

    let MetaAndCtxs(meta, ctxs) = response;
    let mut assets = Vec::with_capacity(meta.universe.len());

    for (index, universe) in meta.universe.iter().enumerate() {
        if universe.isDelisted.unwrap_or(false) {
            continue;
        }

        let Some(ctx) = ctxs.get(index) else { continue };

        let volume = parse_num(ctx.dayNtlVlm.as_deref());

        let mid = parse_num(ctx.midPx.as_deref());
        let prev_day = parse_num(ctx.prevDayPx.as_deref());
        let pnl_24h_pct = if prev_day > 0.0 { (mid - prev_day) / prev_day * 100.0 } else { 0.0 };

        let max_leverage = universe.maxLeverage.unwrap_or(1.0) as u32;

        // Hyperliquid reports open interest in asset units.
        // Convert it to USD notional using mark price when available,
        // otherwise fall back to oracle price and then mid price.
        let oi_contracts = parse_num(ctx.openInterest.as_deref());
        let mark = parse_num(ctx.markPx.as_deref());
        let oracle = parse_num(ctx.oraclePx.as_deref());
        let price_for_oi = if mark > 0.0 { mark } else if oracle > 0.0 { oracle } else { mid };
        let open_interest = oi_contracts * price_for_oi;

        let funding = parse_num(ctx.funding.as_deref());

        let impact_pxs = match &ctx.impactPxs {
            Some(values) => {
                let bid = parse_num(values.first().map(String::as_str));
                let ask = parse_num(values.get(1).map(String::as_str));
                (bid, ask)
            }
            None => (0.0, 0.0),
        };

        assets.push(MarketAsset {
            symbol: universe.name.clone(),
            volume,
            max_leverage,
            pnl_24h_pct,
            open_interest,
            funding,
            impact_pxs,
        });
    }

    // Highest 24h notional volume first.
    assets.sort_by(|a, b| b.volume.total_cmp(&a.volume));

    Ok(assets)
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