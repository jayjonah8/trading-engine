#![allow(dead_code)]

use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Timelike, Utc};
use lz4_flex::frame::FrameDecoder;

use crate::data::historical::hyperliquid::types::HyperliquidFillBlock;


// ============================================================
// HYPERLIQUID HISTORICAL SYMBOL FILTER
// ============================================================

// TODO: Add more granular filtering for each historical market group:
// native perps, individual HIP-3 venues (xyz:, para:, io:, mkts:),
// spot markets (@, trading pairs), and numeric identifiers (#).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidSymbolFilter {
    NativePerps, // Native perpetuals: BTC, ETH, SOL, HYPE, kPEPE, kSHIB, 0G, 2Z
    Hip3Perps,   // HIP-3 perpetuals: xyz:AAPL, para:CRWD, io:IONQ, mkts:US500
    AllPerps,    // Native + HIP-3 perpetuals
    All,         // All markets: native perps, HIP-3 perps, spot (@101, PURR/USDC),
                 // and numeric identifiers (#14870, #126870)
}

impl HyperliquidSymbolFilter {
    fn matches(self, symbol: &str) -> bool {
        let is_hip3 = symbol.contains(':');

        let is_native = !is_hip3
            && !symbol.starts_with('@')
            && !symbol.starts_with('#')
            && !symbol.contains('/');

        match self {
            Self::NativePerps => is_native,
            Self::Hip3Perps => is_hip3,
            Self::AllPerps => is_native || is_hip3,
            Self::All => true,
        }
    }
}


// ============================================================
// DISCOVER HYPERLIQUID HISTORICAL SYMBOLS
// ============================================================

// Scans locally cached Hyperliquid historical fill archives and
// discovers unique symbols within the requested time range.
//
// Historical files are expected under:
//
// ~/trading-data/hyperliquid/node_fills_by_block/hourly/YYYYMMDD/H.lz4
//
// The filter determines which market categories are included.
//
// NOTE:
// Market classification is based on symbol naming conventions.
// NativePerps and AllPerps are approximate filters, not definitive
// historical market-type verification.
//
// Only symbol names are retained in memory. Historical trades are
// not stored.
pub fn discover_hyperliquid_historical_symbols(
    start: &str,
    end: &str,
    filter: HyperliquidSymbolFilter,
) -> Result<Vec<String>> {
    let start = DateTime::parse_from_rfc3339(start)?.with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(end)?.with_timezone(&Utc);

    if start >= end {
        bail!(
            "Invalid Hyperliquid historical range | start={} | end={}",
            start,
            end,
        );
    }

    // ============================================================
    // HISTORICAL CACHE PATH
    // ============================================================

    let home = std::env::var("HOME")?;

    let base_path = PathBuf::from(home)
        .join("trading-data")
        .join("hyperliquid")
        .join("node_fills_by_block")
        .join("hourly");

    // ============================================================
    // DISCOVER SYMBOLS
    // ============================================================

    let mut symbols = HashSet::new();

    let mut hour = start
        .date_naive()
        .and_hms_opt(start.hour(), 0, 0)
        .ok_or_else(|| anyhow::anyhow!("Failed to calculate Hyperliquid start hour"))?
        .and_utc();

    while hour < end {
        let path = base_path
            .join(hour.format("%Y%m%d").to_string())
            .join(format!("{}.lz4", hour.hour()));

        if !path.exists() {
            bail!(
                "Missing Hyperliquid historical archive | file={}",
                path.display(),
            );
        }

        println!(
            "[HYPERLIQUID SYMBOLS] scanning {}",
            path.display(),
        );

        let file = File::open(&path)?;
        let decoder = FrameDecoder::new(file);
        let reader = BufReader::new(decoder);

        for line in reader.lines() {
            let line = line?;

            let block: HyperliquidFillBlock =
                serde_json::from_str(&line)?;

            for (_, fill) in block.events {
                let timestamp =
                    DateTime::<Utc>::from_timestamp_millis(fill.time)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Invalid Hyperliquid historical timestamp | time={}",
                                fill.time,
                            )
                        })?;

                if timestamp < start || timestamp >= end {
                    continue;
                }

                if !filter.matches(&fill.coin) {
                    continue;
                }

                symbols.insert(fill.coin);
            }
        }

        hour += Duration::hours(1);
    }

    // ============================================================
    // SORT DISCOVERED SYMBOLS
    // ============================================================

    let mut symbols: Vec<String> =
        symbols.into_iter().collect();

    symbols.sort();

    println!(
        "[HYPERLIQUID SYMBOLS] discovered {} symbols | filter={:?} | start={} | end={}",
        symbols.len(),
        filter,
        start,
        end,
    );

    Ok(symbols)
}



