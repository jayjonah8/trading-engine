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
// PRICE PERCENT CHANGE
// ============================================================

/// Calculates percentage price change over the specified number
/// of completed candles.
///
/// Uses the opening price of the oldest selected candle and the
/// closing price of the newest selected candle.
///
/// Returns None when insufficient candles, invalid timestamps,
/// or invalid prices exist.
///
/// Examples:
/// - 24 hourly candles = approximately 24-hour price change.
/// - 1440 one-minute candles = approximately 24-hour price change.
/// - 60 one-minute candles = approximately 1-hour price change.
pub fn price_pct_change(
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

    let start_price = selected.first()?.open;
    let end_price = selected.last()?.close;

    if !start_price.is_finite()
        || !end_price.is_finite()
        || start_price <= 0.0
        || end_price <= 0.0
    {
        return None;
    }

    let change = ((end_price / start_price) - 1.0) * 100.0;

    if !change.is_finite() {
        return None;
    }

    Some(change)
}