use anyhow::Result;

use crate::{
    exchange::hyperliquid::api::fetch_hyperliquid_candles,
    market::timeframe::Timeframe,
    shared::print_log,
};

// ============================================================
// HYPERLIQUID CANDLE API TESTS
// ============================================================

// Tests historical candle retrieval from Hyperliquid.
// Validates API responses and logs candle data for inspection.
//
// Arguments:
// - symbol: Trading asset (BTC, ETH, SOL, etc.).
// - timeframe: Candle interval (1m, 5m, 1h, etc.).
// - limit: Number of completed candles to retrieve.
//
// Run:
// cargo test test_fetch_hyperliquid_candles -- --nocapture

#[tokio::test]
async fn test_fetch_hyperliquid_candles() -> Result<()> {
    let debug_mode = true;

    let symbol = "ETH";
    let timeframe = Timeframe::ONE_MINUTE;
    let limit = 30;

    // ========================================================
    // FETCH CANDLES
    // ========================================================

    let candles = fetch_hyperliquid_candles(
        symbol,
        timeframe,
        limit,
    )
    .await?;

    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID CANDLE TEST] symbol={} | timeframe={}s | requested={} | received={}",
            symbol,
            timeframe.as_secs(),
            limit,
            candles.len(),
        ),
    );

    // ========================================================
    // VALIDATE RESPONSE
    // ========================================================

    assert!(
        !candles.is_empty(),
        "Hyperliquid returned no candles"
    );

    assert!(
        candles.len() <= limit,
        "Hyperliquid returned more candles than requested"
    );

    // ========================================================
    // PRINT CANDLES
    // ========================================================

    for candle in &candles {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID CANDLE TEST] {:?}",
                candle,
            ),
        );
    }

    Ok(())
}