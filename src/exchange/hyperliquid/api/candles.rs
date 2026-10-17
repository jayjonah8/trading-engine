#![allow(dead_code)]

use anyhow::{bail, Context, Result};
use chrono::Utc;
use hyperliquid_rust_sdk::CandlesSnapshotResponse;
use reqwest::Client;
use serde_json::json;

use crate::market::timeframe::Timeframe;

// ============================================================
// HYPERLIQUID CANDLE API
// ============================================================

const HYPERLIQUID_INFO_URL: &str =
    "https://api.hyperliquid.xyz/info";

// ============================================================
// FETCH HISTORICAL CANDLES
// ============================================================

/// Fetches the most recent completed candles from Hyperliquid.
///
/// Arguments:
/// - `symbol`: Trading symbol (BTC, ETH, SOL, etc.).
/// - `timeframe`: Candle timeframe.
/// - `limit`: Number of completed candles to request.
///
/// The currently forming candle is excluded.

pub async fn fetch_hyperliquid_candles(
    symbol: &str,
    timeframe: Timeframe,
    limit: usize,
) -> Result<Vec<CandlesSnapshotResponse>> {
    // ========================================================
    // VALIDATE TIMEFRAME
    // ========================================================

    let (interval, interval_ms): (&str, i64) =
        match timeframe.as_secs() {
            60 => ("1m", 60_000),
            180 => ("3m", 180_000),
            300 => ("5m", 300_000),
            900 => ("15m", 900_000),
            1800 => ("30m", 1_800_000),
            3600 => ("1h", 3_600_000),
            7200 => ("2h", 7_200_000),
            14400 => ("4h", 14_400_000),
            28800 => ("8h", 28_800_000),
            43200 => ("12h", 43_200_000),
            86400 => ("1d", 86_400_000),

            seconds => {
                bail!(
                    "Unsupported Hyperliquid timeframe: {} seconds",
                    seconds
                );
            }
        };

    if limit == 0 {
        return Ok(Vec::new());
    }

    // ========================================================
    // CALCULATE TIME RANGE
    // ========================================================

    let now_ms = Utc::now().timestamp_millis();

    let end_ms =
        now_ms.div_euclid(interval_ms) * interval_ms;

    let lookback_ms = i64::try_from(limit)
        .context("Candle limit exceeds i64")?
        .checked_mul(interval_ms)
        .context("Candle lookback overflow")?;

    let start_ms = end_ms
        .checked_sub(lookback_ms)
        .context("Candle start timestamp overflow")?;

    // ========================================================
    // BUILD REQUEST
    // ========================================================

    let body = json!({
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms
        }
    });

    // ========================================================
    // SEND REQUEST
    // ========================================================

    let client = Client::new();

    let response = client
        .post(HYPERLIQUID_INFO_URL)
        .json(&body)
        .send()
        .await
        .context("Hyperliquid candle request failed")?
        .error_for_status()
        .context("Hyperliquid candle API returned an error")?;

    // ========================================================
    // PARSE RESPONSE
    // ========================================================

    let mut candles: Vec<CandlesSnapshotResponse> =
        response
            .json()
            .await
            .context("Failed to deserialize Hyperliquid candles")?;

    // Ensure the caller receives no more than requested.
    // The API may return fewer candles if data is unavailable.

    if candles.len() > limit {
        candles.truncate(limit);
    }

    Ok(candles)
}