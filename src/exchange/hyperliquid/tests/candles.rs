use anyhow::Result;

use crate::{
    exchange::hyperliquid::api::{
        fetch_hyperliquid_candles, 
        fetch_hyperliquid_candles_range
    },

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



// ============================================================
// HYPERLIQUID HISTORICAL CANDLE RANGE TEST
// ============================================================

// Important: Hyperliquid’s candle endpoint has a limited historical retention window 
// (approximately 5,000 candles per interval). Therefore, a request for July 2025 may return 
// no data in October 2026. The function also makes one API request, so very large date ranges 
// may require pagination.

// Tests historical candle retrieval within a specified time range.
// Validates API responses and calculates total asset/USD volume.
//
// Arguments:
// - symbol: Trading asset (BTC, ETH, SOL, etc.).
// - timeframe: Candle interval (1m, 5m, 1h, etc.).
// - start_ms: Start timestamp in Unix milliseconds.
// - end_ms: End timestamp in Unix milliseconds.
//
// Run:
// cargo test test_fetch_hyperliquid_candles_range -- --nocapture

#[tokio::test]
async fn test_fetch_hyperliquid_candles_range() -> Result<()> {
    let debug_mode = true;

    let symbol = "ETH";
    let timeframe = Timeframe::ONE_MINUTE;

    // ========================================================
    // CONFIGURE HISTORICAL TIME RANGE
    // ========================================================

    let start_ms = chrono::DateTime::parse_from_rfc3339(
        "2026-10-15T10:25:00Z",
    )?
    .timestamp_millis();

    let end_ms = chrono::DateTime::parse_from_rfc3339(
        "2026-10-15T10:55:00Z",
    )?
    .timestamp_millis();

    // ========================================================
    // FETCH HISTORICAL CANDLES
    // ========================================================

    let candles = fetch_hyperliquid_candles_range(
        symbol,
        timeframe,
        start_ms,
        end_ms,
    )
    .await?;

    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID CANDLE RANGE TEST] symbol={} | timeframe={}s | received={}",
            symbol,
            timeframe.as_secs(),
            candles.len(),
        ),
    );

    // ========================================================
    // CALCULATE HISTORICAL VOLUME
    // ========================================================

    let mut total_asset_volume = 0.0_f64;
    let mut total_usd_volume = 0.0_f64;

    for candle in &candles {
        let volume: f64 = candle.vlm.parse()?;

        let high: f64 = candle.high.parse()?;
        let low: f64 = candle.low.parse()?;
        let close: f64 = candle.close.parse()?;

        let typical_price = (high + low + close) / 3.0;

        total_asset_volume += volume;
        total_usd_volume += volume * typical_price;

        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID CANDLE RANGE TEST] {:?}",
                candle,
            ),
        );
    }

    // ========================================================
    // PRINT VOLUME SUMMARY
    // ========================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[HYPERLIQUID CANDLE RANGE TEST SUMMARY][{}] CANDLES={} | ASSET_VOLUME={:.4} | EST_USD_VOLUME=${:.2}",
            symbol,
            candles.len(),
            total_asset_volume,
            total_usd_volume,
        ),
    );

    Ok(())
}