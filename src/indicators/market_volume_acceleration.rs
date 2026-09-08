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


/// MARKET VOLUME ACCELERATION
///
/// Measures how quickly synchronized market-wide volume participation
/// is increasing or decreasing.
///
/// For each synchronized asset:
///
///     current RVOL
///     previous RVOL
///
/// Then:
///
///     RVOL change = current RVOL - previous RVOL
///
/// Interpretation:
/// - Positive = participation is increasing.
/// - Negative = participation is decreasing.
/// - Near zero = participation is stable.
///
/// Only assets aligned to the synchronized market snapshot are used.
/// At least min_sync_pct of configured assets must be synchronized.
#[derive(Debug, Clone, Copy)]
pub struct MarketVolumeAcceleration {
    pub current_average_rvol: f64,

    pub previous_average_rvol: f64,

    pub rvol_change: f64,

    pub rvol_change_pct: f64,

    pub accelerating_assets_pct: f64,

    pub assets_used: usize,

    pub assets_total: usize,

    pub sync_pct: f64,

    pub snapshot_end: DateTime<Utc>,
}


pub fn market_volume_acceleration(
    history: &CandleHistory,
    symbols: &[String],
    timeframe: Timeframe,
    volume_period: usize,
    min_sync_pct: f64,
) -> Option<MarketVolumeAcceleration> {
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


    let mut current_total =
        0.0;

    let mut previous_total =
        0.0;

    let mut accelerating_assets =
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
            < volume_period + 2
        {
            continue;
        }


        let len =
            bars.len();


        if bars[len - 1].end
            != snapshot.snapshot_end
        {
            continue;
        }


        let current_volume =
            bars[len - 1]
                .volume as f64;

        let previous_volume =
            bars[len - 2]
                .volume as f64;


        let current_average =
            bars.iter()
                .skip(
                    len
                        - 1
                        - volume_period
                )
                .take(volume_period)
                .map(|bar| {
                    bar.volume as f64
                })
                .sum::<f64>()
                / volume_period as f64;


        let previous_average =
            bars.iter()
                .skip(
                    len
                        - 2
                        - volume_period
                )
                .take(volume_period)
                .map(|bar| {
                    bar.volume as f64
                })
                .sum::<f64>()
                / volume_period as f64;


        if current_average <= 0.0
            || previous_average <= 0.0
        {
            continue;
        }


        let current_rvol =
            current_volume
                / current_average;

        let previous_rvol =
            previous_volume
                / previous_average;


        current_total +=
            current_rvol;

        previous_total +=
            previous_rvol;

        assets_used +=
            1;


        if current_rvol
            > previous_rvol
        {
            accelerating_assets +=
                1;
        }
    }


    if assets_used == 0 {
        return None;
    }


    let current_average_rvol =
        current_total
            / assets_used as f64;

    let previous_average_rvol =
        previous_total
            / assets_used as f64;


    let rvol_change =
        current_average_rvol
            - previous_average_rvol;


    let rvol_change_pct =
        if previous_average_rvol > 0.0 {
            (
                rvol_change
                    / previous_average_rvol
            )
                * 100.0
        } else {
            0.0
        };


    Some(
        MarketVolumeAcceleration {
            current_average_rvol,

            previous_average_rvol,

            rvol_change,

            rvol_change_pct,

            accelerating_assets_pct:
                (
                    accelerating_assets as f64
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