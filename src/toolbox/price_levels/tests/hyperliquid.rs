use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hyperliquid_rust_sdk::CandlesSnapshotResponse;

use crate::{
    core::events::Bar,
    exchange::hyperliquid::api::candles::fetch_hyperliquid_candles,
    market::timeframe::Timeframe,
};

// ====================================================
// HYPERLIQUID PRICE LEVEL TEST DATA
// ====================================================
//
// Fetches real completed historical candles from
// Hyperliquid and converts them into the engine's
// internal Bar representation.
//
// The resulting candles can be passed directly into
// CandleHistory and PriceLevelDetector.

// ====================================================
// CONVERT HYPERLIQUID CANDLE
// ====================================================

fn convert_candle(
    candle: CandlesSnapshotResponse,
    timeframe: Timeframe,
) -> Result<Bar> {

    let start_ms = i64::try_from(candle.time_open)
        .context("Candle opening timestamp exceeds i64")?;

    let start = DateTime::<Utc>::from_timestamp_millis(start_ms)
        .context("Invalid candle opening timestamp")?;

    // Hyperliquid's closing timestamp is inclusive.
    // Convert it to the exclusive candle end timestamp
    // used by the engine.

    let end_ms = i64::try_from(candle.time_close)
        .context("Candle closing timestamp exceeds i64")?
        .checked_add(1)
        .context("Candle closing timestamp overflow")?;

    let end = DateTime::<Utc>::from_timestamp_millis(end_ms)
        .context("Invalid candle closing timestamp")?;

    let open = candle.open.parse::<f64>()
        .context("Invalid candle open price")?;

    let high = candle.high.parse::<f64>()
        .context("Invalid candle high price")?;

    let low = candle.low.parse::<f64>()
        .context("Invalid candle low price")?;

    let close = candle.close.parse::<f64>()
        .context("Invalid candle close price")?;

    let volume = candle.vlm.parse::<f64>()
        .context("Invalid candle volume")?;

    Ok(Bar {
        symbol: candle.coin,
        timeframe,
        start,
        end,
        open,
        high,
        low,
        close,
        volume,
        trade_count: candle.num_trades,

        // Exact trade-level price-volume sum is not
        // provided by the Hyperliquid candle API.
        // The detector does not currently use this field.
        price_volume_sum: 0.0,
    })
}

// ====================================================
// FETCH TEST CANDLES
// ====================================================

pub async fn fetch_test_candles(
    symbol: &str,
    timeframe: Timeframe,
    limit: usize,
) -> Result<Vec<Bar>> {

    let candles = fetch_hyperliquid_candles(
        symbol,
        timeframe,
        limit,
    )
    .await?;

    let mut bars = Vec::with_capacity(candles.len());

    for candle in candles {
        bars.push(convert_candle(candle, timeframe)?);
    }

    // Ensure chronological ordering.

    bars.sort_by_key(|bar| bar.start);

    // Avoid duplicate candle timestamps.

    bars.dedup_by_key(|bar| bar.start);

    Ok(bars)
}