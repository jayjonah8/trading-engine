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


/// MARKET BREADTH
///
/// Measures how many synchronized tracked assets moved upward versus
/// downward over the same lookback window.
///
/// For each synchronized asset:
///
///     net move = latest close vs close N candles ago
///
/// Returns:
/// - bullish percentage
/// - bearish percentage
/// - neutral percentage
/// - average market move
///
/// Useful for:
/// - confirming broad market momentum
/// - distinguishing broad moves from isolated moves
/// - identifying risk-on / risk-off behavior
/// - measuring participation in market trends
///
/// At least min_sync_pct of configured assets must be aligned to the
/// same latest candle timestamp before the calculation can run.
#[derive(Debug, Clone, Copy)]
pub struct MarketBreadth {
    pub bullish_assets_pct: f64,

    pub bearish_assets_pct: f64,

    pub neutral_assets_pct: f64,

    pub average_move_pct: f64,

    pub assets_used: usize,

    pub assets_total: usize,

    pub sync_pct: f64,

    pub snapshot_end: DateTime<Utc>,
}


pub fn market_breadth(
    history: &CandleHistory,
    symbols: &[String],
    timeframe: Timeframe,
    period: usize,
    min_sync_pct: f64,
) -> Option<MarketBreadth> {
    if period == 0
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


    let mut bullish =
        0usize;

    let mut bearish =
        0usize;

    let mut neutral =
        0usize;

    let mut total_move =
        0.0;

    let mut assets_used =
        0usize;


    const EPS: f64 =
        1e-8;


    for symbol in
        &snapshot.symbols
    {
        let bars =
            history.get(
                symbol,
                timeframe,
            )?;


        if bars.len()
            < period + 1
        {
            continue;
        }


        let latest_bar =
            bars.back()?;


        if latest_bar.end
            != snapshot.snapshot_end
        {
            continue;
        }


        let latest =
            latest_bar.close;


        let old =
            bars[
                bars.len()
                    - 1
                    - period
            ]
                .close;


        if old <= 0.0 {
            continue;
        }


        let move_pct =
            (
                (latest - old)
                    / old
            )
                * 100.0;


        total_move +=
            move_pct;

        assets_used +=
            1;


        if move_pct > EPS {
            bullish +=
                1;
        } else if move_pct < -EPS {
            bearish +=
                1;
        } else {
            neutral +=
                1;
        }
    }


    if assets_used == 0 {
        return None;
    }


    Some(
        MarketBreadth {
            bullish_assets_pct:
                (
                    bullish as f64
                        / assets_used as f64
                )
                    * 100.0,

            bearish_assets_pct:
                (
                    bearish as f64
                        / assets_used as f64
                )
                    * 100.0,

            neutral_assets_pct:
                (
                    neutral as f64
                        / assets_used as f64
                )
                    * 100.0,

            average_move_pct:
                total_move
                    / assets_used as f64,

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