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
// USD VOLUME
// ============================================================

/// Calculates total USD trading volume over the specified
/// number of completed candles.
///
/// Uses Bar::price_volume_sum, accumulated from the original
/// raw tape trades during candle construction.
///
/// Returns None when insufficient candles, invalid timestamps,
/// or invalid USD volume exists.
///
/// Examples:
/// - 24 hourly candles = approximately 24-hour USD volume.
/// - 1440 one-minute candles = approximately 24-hour USD volume.
/// - 60 one-minute candles = approximately 1-hour USD volume.
pub fn volume_usd(
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

    let mut total_volume_usd = 0.0;

    for bar in selected {
        if !bar.price_volume_sum.is_finite()
            || bar.price_volume_sum < 0.0
        {
            return None;
        }

        total_volume_usd += bar.price_volume_sum;
    }

    if !total_volume_usd.is_finite() {
        return None;
    }

    Some(total_volume_usd)
}