use chrono::{
    DateTime,
    Utc,
};

use crate::{
    indicators::synchronized_snapshot,

    market::{
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },
};


/// MARKET VOLUME EXPANSION
///
/// Measures whether unusually high trading volume is appearing across
/// many tracked assets at the same synchronized market timestamp.
///
/// For each synchronized asset:
///
///     RVOL = latest candle volume / average previous volume
///
/// The individual RVOL values are then combined into a market-wide
/// participation measurement.
///
/// Useful for:
/// - detecting broad market participation
/// - detecting market-wide volume expansion
/// - distinguishing isolated volume spikes from broad activity
/// - identifying environments that may support stronger trends
///
/// The calculation only uses assets synchronized to the same candle
/// timestamp and requires at least min_sync_pct of configured assets.
#[derive(Debug, Clone, Copy)]
pub struct MarketVolumeExpansion {
    pub average_rvol: f64,

    pub expanding_assets_pct: f64,

    pub assets_used: usize,

    pub assets_total: usize,

    pub sync_pct: f64,

    pub snapshot_end: DateTime<Utc>,
}


pub fn market_volume_expansion(
    history: &CandleHistory,
    symbols: &[String],
    timeframe: Timeframe,
    volume_period: usize,
    expansion_threshold: f64,
    min_sync_pct: f64,
) -> Option<MarketVolumeExpansion> {
    if volume_period == 0
        || symbols.is_empty()
    {
        return None;
    }


    let snapshot =
        synchronized_snapshot(
            history,
            symbols,
            timeframe,
            min_sync_pct,
        )?;


    let mut total_rvol =
        0.0;

    let mut expanding_assets =
        0usize;

    let mut assets_used =
        0usize;


    for symbol in
        &snapshot.symbols
    {
        let bars =
            history.get(
                symbol,
                timeframe,
            )?;


        if bars.len()
            < volume_period + 1
        {
            continue;
        }


        let current =
            bars.back()?;


        // Extra safety:
        // only use the synchronized candle.
        if current.end
            != snapshot.snapshot_end
        {
            continue;
        }


        let history_end =
            bars.len() - 1;

        let history_start =
            history_end
                - volume_period;


        let average_volume =
            bars.iter()
                .skip(history_start)
                .take(volume_period)
                .map(|bar| {
                    bar.volume as f64
                })
                .sum::<f64>()
                / volume_period as f64;


        if average_volume <= 0.0 {
            continue;
        }


        let rvol =
            current.volume as f64
                / average_volume;


        total_rvol +=
            rvol;

        assets_used +=
            1;


        if rvol
            >= expansion_threshold
        {
            expanding_assets +=
                1;
        }
    }


    if assets_used == 0 {
        return None;
    }


    Some(
        MarketVolumeExpansion {
            average_rvol:
                total_rvol
                    / assets_used as f64,

            expanding_assets_pct:
                (
                    expanding_assets as f64
                        / assets_used as f64
                )
                    * 100.0,

            assets_used,

            assets_total:
                snapshot.assets_total,

            sync_pct:
                snapshot.sync_pct,

            snapshot_end:
                snapshot.snapshot_end,
        }
    )
}