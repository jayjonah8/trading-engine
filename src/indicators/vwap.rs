use chrono::{
    DateTime,
    Utc,
};

use crate::core::events::Bar;

/// VOLUME WEIGHTED AVERAGE PRICE (VWAP)
///
/// VWAP calculates the average traded price weighted by actual trade
/// volume. Prices where more quantity traded contribute more heavily.
///
/// This implementation uses exact raw-trade accumulation stored in each Bar:
///
///     price_volume_sum = Σ(trade_price * trade_size)
///
/// VWAP is then:
///
///     VWAP = Σ(price_volume_sum) / Σ(volume)
///
/// This is more accurate than approximating VWAP from candle OHLC values
/// because the calculation preserves the actual price and size of the
/// raw trades used to construct each candle.
///
/// Volume uses `f64` because quantities may be fractional across venues,
/// such as crypto or fractional equities.
///
/// Common uses:
/// - Measure the market's volume-weighted average traded price.
/// - Determine whether current price is above or below session VWAP.
/// - Identify potential intraday support/resistance.
/// - Measure price extension away from the session's average traded price.
///
/// Interpretation:
/// - Price > VWAP = price is above the volume-weighted average.
/// - Price < VWAP = price is below the volume-weighted average.
/// - Price near VWAP = price is trading near its volume-weighted mean.
///
/// Session VWAP:
/// Normal intraday VWAP resets at the beginning of each trading session.
/// Only bars belonging to the selected session should contribute to that
/// session's VWAP.
///
/// Because Bar stores exact `price_volume_sum` from raw trades, the same
/// calculation works consistently across live trading and historical replay.
pub fn vwap(
    bars: &[Bar],
) -> Option<f64> {
    if bars.is_empty() {
        return None;
    }

    let total_price_volume: f64 =
        bars.iter()
            .map(|bar| {
                bar.price_volume_sum
            })
            .sum();

    let total_volume: f64 =
        bars.iter()
            .map(|bar| {
                bar.volume
            })
            .sum();

    // A VWAP cannot be calculated when no volume traded.
    if total_volume <= 0.0 {
        return None;
    }

    Some(
        total_price_volume
            / total_volume
    )
}

/// Exact VWAP using only candles inside a specific time window.
///
/// Useful for:
/// - regular trading session VWAP
/// - morning VWAP
/// - custom rolling windows
/// - historical replay session boundaries
///
/// The timestamps stored on each Bar determine whether it belongs to the
/// requested window, so this function is independent of live/backtest mode.
pub fn vwap_between(
    bars: &[Bar],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Option<f64> {
    let mut total_price_volume =
        0.0;

    let mut total_volume =
        0.0;

    for bar in bars {
        // Ignore candles that do not overlap the requested time window.
        if bar.end <= start
            || bar.start >= end
        {
            continue;
        }

        total_price_volume +=
            bar.price_volume_sum;

        total_volume +=
            bar.volume;
    }

    if total_volume <= 0.0 {
        return None;
    }

    Some(
        total_price_volume
            / total_volume
    )
}

// Waring: Keep an eye on this
// And importantly, you don’t need separate live and backtest session-VWAP implementations. 
// Both use the timestamp carried by the candle, so when replaying August 3 historical data, 
// it determines the August 3 session; during live trading, it determines the current market session.