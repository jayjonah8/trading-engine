use crate::core::events::Bar;
use crate::market::utils::{
    valid_candle_window,
    DEFAULT_CANDLE_TOLERANCE,
};


// ============================================================
// STRATEGY-LEVEL CANDLE VALIDATION
// ============================================================

// The volume indicators (volume_usd(), volume_asset()) and
// price_pct_change() use valid_candle_window() to validate candle
// count, freshness, and historical window tolerance.
//
// Strategies can use these indicators in their can_trade checks
// to ensure the required market data is sufficiently recent.
//
// Other indicators (ATR, RSI, SMA, etc.) retain their existing
// validation and return None when insufficient candles are available.
//
// This avoids unnecessary duplicate timestamp validation across
// every indicator while keeping the strategy responsible for
// checking that all required indicator results are valid.
//
// IMPORTANT:
// Validation applies only to the specific candle timeframe and
// historical window being checked. Other timeframes are not
// automatically validated.


// ============================================================
// ASSET VOLUME
// ============================================================

/// Calculates total traded asset volume over the specified
/// number of completed candles.
///
/// Uses Bar::volume, representing the quantity of the underlying
/// asset traded during candle construction.
///
/// Returns None when insufficient candles, invalid timestamps,
/// or invalid volume exists.
///
/// Examples:
/// - 24 hourly candles = approximately 24-hour asset volume.
/// - 1440 one-minute candles = approximately 24-hour asset volume.
/// - 60 one-minute candles = approximately 1-hour asset volume.
pub fn volume_asset(
    bars: &[Bar],
    period: usize,
    current_ts: i64,
) -> Option<f64> {
    if period == 0 || bars.len() < period {
        return None;
    }

    if !valid_candle_window(bars, period, current_ts, DEFAULT_CANDLE_TOLERANCE) {
        return None;
    }

    let selected = &bars[bars.len() - period..];

    let mut total_volume = 0.0;

    for bar in selected {
        if !bar.volume.is_finite() || bar.volume < 0.0 {
            return None;
        }

        total_volume += bar.volume;
    }

    if !total_volume.is_finite() {
        return None;
    }

    Some(total_volume)
}