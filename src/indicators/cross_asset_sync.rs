use std::collections::HashMap;

use chrono::{
    DateTime,
    Utc,
};

use crate::market::{
    candle_history::CandleHistory,
    timeframe::Timeframe,
};


/// CROSS-ASSET CANDLE SYNCHRONIZATION
///
/// Finds the newest completed candle timestamp shared by at least
/// the configured percentage of tracked assets.
///
/// This allows market-wide indicators to stay synchronized without
/// requiring every single asset to always have a fresh candle.
///
/// Example:
///
/// Required synchronization = 80%
///
/// AAPL -> 13:35
/// MSFT -> 13:35
/// NVDA -> 13:35
/// TSLA -> 13:30
/// META -> 13:35
///
/// 4 / 5 assets are aligned at 13:35 = 80%
///
/// Result:
///     valid snapshot at 13:35
///
/// TSLA is excluded from that particular market-wide calculation.
///
/// If fewer than the required percentage are aligned, None is returned.
///
/// This works in both live and backtest modes because synchronization
/// uses candle market timestamps rather than wall-clock timing.
#[derive(Debug, Clone)]
pub struct CrossAssetSnapshot {
    // Shared candle end timestamp used by the snapshot.
    pub snapshot_end: DateTime<Utc>,

    // Symbols that are synchronized at snapshot_end.
    pub symbols: Vec<String>,

    // Number of synchronized assets.
    pub assets_synced: usize,

    // Total number of configured assets.
    pub assets_total: usize,

    // Percentage of configured assets synchronized.
    pub sync_pct: f64,
}


pub fn synchronized_snapshot(
    history: &CandleHistory,
    symbols: &[String],
    timeframe: Timeframe,
    min_sync_pct: f64,
) -> Option<CrossAssetSnapshot> {
    if symbols.is_empty()
        || !min_sync_pct.is_finite()
        || min_sync_pct <= 0.0
        || min_sync_pct > 100.0
    {
        return None;
    }


    // Count how many tracked symbols currently have each
    // latest completed candle end timestamp.
    let mut timestamp_symbols:
        HashMap<DateTime<Utc>, Vec<String>> =
        HashMap::new();


    for symbol in symbols {
        let bars =
            match history.get(
                symbol,
                timeframe,
            ) {
                Some(bars) => bars,

                None => continue,
            };


        let latest =
            match bars.back() {
                Some(bar) => bar,

                None => continue,
            };


        timestamp_symbols
            .entry(latest.end)
            .or_default()
            .push(symbol.clone());
    }


    if timestamp_symbols.is_empty() {
        return None;
    }


    let assets_total =
        symbols.len();


    // Find the newest timestamp that satisfies the required
    // percentage of synchronized assets.
    let mut candidates:
        Vec<_> =
        timestamp_symbols
            .into_iter()
            .collect();


    candidates.sort_by(
        |a, b| {
            b.0.cmp(&a.0)
        }
    );


    for (
        snapshot_end,
        synced_symbols,
    ) in candidates
    {
        let assets_synced =
            synced_symbols.len();


        let sync_pct =
            (
                assets_synced as f64
                    / assets_total as f64
            )
                * 100.0;


        if sync_pct >= min_sync_pct {
            return Some(
                CrossAssetSnapshot {
                    snapshot_end,

                    symbols:
                        synced_symbols,

                    assets_synced,

                    assets_total,

                    sync_pct,
                }
            );
        }
    }


    None
}