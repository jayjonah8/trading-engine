#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, Timelike, Utc};
use lz4_flex::frame::FrameDecoder;

use crate::data::historical::{
    hyperliquid::types::HyperliquidFillBlock,
    types::HistoricalTrade,
};


// ============================================================
// HYPERLIQUID HISTORICAL TRADE FETCHER
// ============================================================

// Reads Hyperliquid's locally cached `node_fills_by_block` archive and
// normalizes the raw account-side fills into shared `HistoricalTrade` values.
//
// Historical files are expected under:
//
// ~/trading-data/hyperliquid/node_fills_by_block/hourly/YYYYMMDD/H.lz4
//
// Each hourly archive contains both participant fills for an execution.
// `tid` identifies the underlying market trade, so duplicate participant
// fills are collapsed into one `HistoricalTrade`.
//
// TODO:
// Add optional automatic S3 downloading for missing Hyperliquid historical
// archive files. For now, historical data is downloaded ahead of time and
// cached locally so backtests operate entirely against local data.
//
// TODO:
// The Alpaca and Hyperliquid backtest runners currently remain separate for
// simplicity. Later, extract their common candle/replay pipeline into a
// provider-independent historical backtest runner.
pub fn fetch_hyperliquid_historical_trades(
    symbols: &[&str],
    start: &str,
    end: &str,
) -> Result<Vec<HistoricalTrade>, Box<dyn std::error::Error>> {
    let start = DateTime::parse_from_rfc3339(start)?.with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(end)?.with_timezone(&Utc);

    if start >= end {
        return Err(format!("Invalid Hyperliquid historical range | start={} | end={}", start, end).into());
    }

    if symbols.is_empty() {
        return Err("Hyperliquid historical symbols cannot be empty".into());
    }

    let requested_symbols: HashSet<&str> = symbols.iter().copied().collect();
    let base_path = hyperliquid_historical_base_path()?;

    let mut trades_by_tid: HashMap<u64, HistoricalTrade> = HashMap::new();
    let mut hour = start.date_naive().and_hms_opt(start.hour(), 0, 0).ok_or("Failed to calculate Hyperliquid start hour")?.and_utc();

    while hour < end {
        let path = historical_hour_path(&base_path, hour);

        if !path.exists() {
            return Err(format!("Missing Hyperliquid historical archive | file={}", path.display()).into());
        }

        println!("[HYPERLIQUID HISTORICAL] reading {}", path.display());

        read_historical_hour(
            &path,
            &requested_symbols,
            start,
            end,
            &mut trades_by_tid,
        )?;

        hour += Duration::hours(1);
    }

    let mut trades: Vec<HistoricalTrade> = trades_by_tid.into_values().collect();

    // TODO:
    // Preserve deterministic archive ordering for trades with identical timestamps.
    // Timestamp ordering is sufficient for the current medium-frequency directional
    // backtester, but exact same-timestamp ordering may matter for future HFT,
    // fine-grained tick strategies, market-making simulation, queue-position modeling,
    // order-book reconstruction, or millisecond/sub-millisecond execution simulation,
    // where exact event sequence can affect observed prices and whether orders fill.
    trades.sort_by_key(|trade| trade.timestamp);

    println!(
        "[HYPERLIQUID HISTORICAL] loaded {} trades | symbols={:?} | start={} | end={}",
        trades.len(),
        symbols,
        start,
        end,
    );

    Ok(trades)
}


// ============================================================
// READ HISTORICAL HOUR
// ============================================================

fn read_historical_hour(
    path: &Path,
    requested_symbols: &HashSet<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    trades_by_tid: &mut HashMap<u64, HistoricalTrade>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let decoder = FrameDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut skipped_invalid_fills = 0usize;

    for line in reader.lines() {
        let line = line?;
        let block: HyperliquidFillBlock = serde_json::from_str(&line)?;

        for (_, fill) in block.events {
            if !requested_symbols.contains(fill.coin.as_str()) {
                continue;
            }

            let timestamp = DateTime::<Utc>::from_timestamp_millis(fill.time)
                .ok_or_else(|| format!(
                    "Invalid Hyperliquid historical timestamp | time={}",
                    fill.time,
                ))?;

            if timestamp < start || timestamp >= end {
                continue;
            }

            let price = fill.px.parse::<f64>()?;
            let size = fill.sz.parse::<f64>()?;

            // ============================================================
            // SKIP INVALID HISTORICAL FILLS
            // ============================================================

            if !price.is_finite() || price <= 0.0
                || !size.is_finite() || size <= 0.0
            {
                skipped_invalid_fills += 1;
                continue;
            }

            let trade = HistoricalTrade {
                symbol: fill.coin,
                timestamp,
                price,
                size,
            };

            if let Some(existing) = trades_by_tid.get(&fill.tid) {
                validate_duplicate_trade(fill.tid, existing, &trade)?;
                continue;
            }

            trades_by_tid.insert(fill.tid, trade);
        }
    }

    // ============================================================
    // INVALID FILL SUMMARY
    // ============================================================

    if skipped_invalid_fills > 0 {
        println!(
            "[HYPERLIQUID HISTORICAL] skipped {} invalid fills | file={}",
            skipped_invalid_fills,
            path.display(),
        );
    }

    Ok(())
}


// ============================================================
// DUPLICATE TRADE VALIDATION
// ============================================================

// Hyperliquid stores one fill for each participant in an execution.
// Both participant fills should describe the same underlying market trade.
fn validate_duplicate_trade(
    tid: u64,
    existing: &HistoricalTrade,
    duplicate: &HistoricalTrade,
) -> Result<(), Box<dyn std::error::Error>> {
    if existing.symbol != duplicate.symbol
        || existing.timestamp != duplicate.timestamp
        || existing.price != duplicate.price
        || existing.size != duplicate.size
    {
        return Err(format!(
            "Hyperliquid historical tid mismatch | tid={} | existing={:?} | duplicate={:?}",
            tid,
            existing,
            duplicate,
        ).into());
    }

    Ok(())
}


// ============================================================
// HISTORICAL CACHE PATHS
// ============================================================

fn hyperliquid_historical_base_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let home = std::env::var("HOME")?;

    Ok(
        PathBuf::from(home)
            .join("trading-data")
            .join("hyperliquid")
            .join("node_fills_by_block")
            .join("hourly")
    )
}

fn historical_hour_path(
    base_path: &Path,
    hour: DateTime<Utc>,
) -> PathBuf {
    base_path
        .join(hour.format("%Y%m%d").to_string())
        .join(format!("{}.lz4", hour.hour()))
}