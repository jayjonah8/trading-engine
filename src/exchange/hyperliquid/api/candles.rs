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



// ============================================================
// FETCH HISTORICAL CANDLES BY TIME RANGE
// ============================================================

// Important: Hyperliquid's candle endpoint has a limited historical
// retention window (approximately 5,000 candles per interval).
// Older historical data may no longer be available.
// Large time ranges may require pagination.

/// Fetches historical candles within a specified UTC time range.
///
/// Arguments:
/// - `symbol`: Trading symbol (BTC, ETH, SOL, etc.).
/// - `timeframe`: Candle timeframe.
/// - `start_ms`: Start timestamp in Unix milliseconds (inclusive).
/// - `end_ms`: End timestamp in Unix milliseconds (exclusive).

pub async fn fetch_hyperliquid_candles_range(
    symbol: &str,
    timeframe: Timeframe,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<CandlesSnapshotResponse>> {
    // ========================================================
    // VALIDATE TIMEFRAME
    // ========================================================

    let interval = match timeframe.as_secs() {
        60 => "1m",
        180 => "3m",
        300 => "5m",
        900 => "15m",
        1800 => "30m",
        3600 => "1h",
        7200 => "2h",
        14400 => "4h",
        28800 => "8h",
        43200 => "12h",
        86400 => "1d",

        seconds => {
            bail!(
                "Unsupported Hyperliquid timeframe: {} seconds",
                seconds
            );
        }
    };

    // ========================================================
    // VALIDATE TIME RANGE
    // ========================================================

    if start_ms < 0 || end_ms < 0 {
        bail!("Timestamps cannot be negative");
    }

    if start_ms >= end_ms {
        bail!("Start timestamp must be before end timestamp");
    }

    // Convert chrono i64 timestamps to SDK-compatible u64.
    let start_ms_u64 = start_ms as u64;
    let end_ms_u64 = end_ms as u64;

    // ========================================================
    // BUILD REQUEST
    // ========================================================

    let body = json!({
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": interval,
            "startTime": start_ms_u64,
            "endTime": end_ms_u64
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
        .context("Hyperliquid historical candle request failed")?
        .error_for_status()
        .context("Hyperliquid historical candle API returned an error")?;

    // ========================================================
    // PARSE RESPONSE
    // ========================================================

    let mut candles: Vec<CandlesSnapshotResponse> = response
        .json()
        .await
        .context("Failed to deserialize historical candles")?;

    // ========================================================
    // FILTER REQUESTED TIME RANGE
    // ========================================================

    candles.retain(|candle| {
        candle.time_open >= start_ms_u64
            && candle.time_close < end_ms_u64
    });

    // ========================================================
    // SORT CANDLES CHRONOLOGICALLY
    // ========================================================

    candles.sort_by_key(|candle| candle.time_open);

    Ok(candles)
}