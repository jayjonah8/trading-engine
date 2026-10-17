#![allow(dead_code)]

use crate::core::events::Bar;
use chrono::Duration;


/// Default candle window tolerance (3%).
/// Re-used inside indicators in other files.
pub const DEFAULT_CANDLE_TOLERANCE: f64 = 0.03;


// ============================================================
// CANDLE VALIDATION CONFIGURATION
// ============================================================

/// Master switch for candle validation.
///
/// true  = Enable all candle validation safeguards.
/// false = Skip validation and immediately return true.
///
/// Note:
/// Disabling validation does not disable the individual indicator's
/// own checks for insufficient candles or invalid numeric values.
pub const ENABLE_CANDLE_VALIDATION: bool = true;


// ============================================================
// CANDLE WINDOW VALIDATION
// ============================================================

/// Validates candle history using Unix millisecond timestamps.
///
/// PURPOSE:
/// Ensures that indicators calculate values using sufficiently
/// recent candles without including excessively old historical data.
///
/// This is especially important for volume indicators because
/// missing candle intervals could cause older candles containing
/// historical trading volume to enter the calculation.
///
/// VALIDATION CHECKS:
///
/// 1. CANDLE COUNT
///    - The requested period must be greater than zero.
///    - Enough completed candles must exist.
///    - Example: period = 1440 requires at least 1440 candles.
///
/// 2. TOLERANCE VALIDATION
///    - Tolerance must be a finite, nonnegative number.
///    - Each indicator can configure its own tolerance.
///    - Example: tolerance = 0.021 means 2.1%.
///
/// 3. TIMEFRAME DURATION
///    - Determines the expected duration of each candle.
///    - Example: ONE_MINUTE = 60 seconds.
///    - Example: ONE_HOUR = 3600 seconds.
///
/// 4. EXPECTED WINDOW
///    - Calculates the expected duration of the selected candles.
///    - Formula: candle_duration * period.
///    - Example: 1440 * 1 minute = 24 hours.
///
/// 5. HISTORICAL WINDOW TOLERANCE
///    - Allows a small amount of additional historical time.
///    - Formula: expected_duration * (1.0 + tolerance).
///    - Example: 24 hours with 2.1% tolerance = 24h 30m 14.4s.
///    - Prevents excessively old candles from entering calculations.
///
/// 6. FUTURE TIMESTAMP PROTECTION
///    - Rejects candles ending after the current market-event time.
///    - Prevents using future candle data during historical backtesting.
///    - Example: current_ts = 12:00, last.end = 12:05 => REJECT.
///
/// 7. STALE CANDLE PROTECTION
///    - Rejects candles whose latest timestamp is too old.
///    - Maximum age = candle_duration * 2.
///    - Example: 1-minute candles may be up to 2 minutes old.
///    - Example: 1-hour candles may be up to 2 hours old.
///    - Prevents indicators from using outdated market data.
///
/// 8. OLDEST CANDLE PROTECTION
///    - Measures the total time between the oldest selected
///      candle's start and the newest selected candle's end.
///    - Rejects the window if it exceeds the allowed duration.
///    - Prevents large gaps from pulling excessively old candles
///      containing historical volume into the calculation.
///
/// ============================================================
/// BEHAVIOR EXAMPLES
/// ============================================================
///
/// Example configuration:
///     period = 1440
///     timeframe = ONE_MINUTE
///     tolerance = 0.021
///
/// Expected window: 24 hours.
/// Maximum allowed window: approximately 24 hours + 30 minutes.
///
/// CANDLE WINDOW EXAMPLES:
///
///     1440 candles spanning 24 hours       => ACCEPT
///     1440 candles spanning 24h 10m        => ACCEPT
///     1440 candles spanning 24h 20m        => ACCEPT
///     1440 candles spanning 24h 30m        => ACCEPT
///     1440 candles spanning 24h 45m        => REJECT
///     1440 candles spanning 25 hours       => REJECT
///     Only 1000 candles available          => REJECT
///     Period = 0                           => REJECT
///     Negative or NaN tolerance            => REJECT
///
/// LATEST CANDLE FRESHNESS EXAMPLES:
///
///     Latest 1-minute candle is 30s old    => ACCEPT
///     Latest 1-minute candle is 1m old     => ACCEPT
///     Latest 1-minute candle is 2m old     => ACCEPT
///     Latest 1-minute candle is 3m old     => REJECT
///     Latest 1-minute candle is 5m old     => REJECT
///     Latest candle ends in the future     => REJECT
///
/// HISTORICAL VOLUME EXAMPLES:
///
///     Missing 5 minutes:
///         Older candles may be included.
///         Total window remains within tolerance.
///         Result: ACCEPT.
///
///     Missing 45 minutes:
///         Older candles would extend the window too far.
///         Result: REJECT.
///
///     Entire dataset is several days old:
///         Latest candle fails the freshness check.
///         Result: REJECT.
///
/// ============================================================
/// IMPORTANT LIMITATIONS
/// ============================================================
///
/// - Assumes CandleHistory maintains chronological candle ordering.
/// - Does not individually validate every candle's timestamps.
/// - Does not independently detect overlapping or duplicate candles.
/// - Does not independently verify every candle uses the same timeframe.
/// - Small gaps within tolerance can include slightly older volume.
/// - The result represents an approximate rather than exact time window.
/// - Tolerance applies proportionally to any requested timeframe/window.
///
/// - These concerns should already be handled when generating candles
///   in backtest mode or live mode.
///
/// All ACCEPT examples assume the other validation checks also pass.
///
/// BACKTEST COMPATIBILITY:
///
/// current_ts must represent the current market-event timestamp
/// in Unix milliseconds, NOT the computer's real-world clock.
///
/// Live trading:
///     current_ts = quote.timestamp_ms
///
/// Historical backtesting:
///     current_ts = trade.timestamp_ms
///
/// Both timestamps are already normalized to Unix milliseconds
/// by their respective market-data providers or backtest runners.
///
/// This ensures identical validation behavior in both environments.
pub fn valid_candle_window(
    bars: &[Bar],
    period: usize,
    current_ts: i64,
    tolerance: f64,
) -> bool {

    // ============================================================
    // 1. GLOBAL VALIDATION SWITCH
    // ============================================================

    // Skip all validation when globally disabled.
    if !ENABLE_CANDLE_VALIDATION {
        return true;
    }

    // ============================================================
    // 2. CANDLE COUNT VALIDATION
    // ============================================================

    // Reject zero periods or insufficient candle history.
    if period == 0 || bars.len() < period {
        return false;
    }

    // ============================================================
    // 3. TOLERANCE VALIDATION
    // ============================================================

    // Reject negative, infinite, or NaN tolerance values.
    if !tolerance.is_finite() || tolerance < 0.0 {
        return false;
    }

    // ============================================================
    // 4. SELECT CANDLE WINDOW
    // ============================================================

    // Select only the most recent 'period' completed candles.
    let selected = &bars[bars.len() - period..];

    // Oldest selected candle.
    let first = &selected[0];

    // Newest selected candle.
    let last = &selected[selected.len() - 1];

    // ============================================================
    // 5. CALCULATE EXPECTED WINDOW DURATION
    // ============================================================

    // Convert the candle timeframe into a Chrono duration.
    let Ok(candle_duration) =
        Duration::from_std(first.timeframe.duration())
    else {
        return false;
    };

    // Safely convert the requested period into an integer.
    let Ok(period_i32) = i32::try_from(period) else {
        return false;
    };

    // Calculate the expected duration of the selected candles.
    //
    // Example:
    // 1440 one-minute candles = 1440 minutes = 24 hours.
    let Some(expected_duration) =
        candle_duration.checked_mul(period_i32)
    else {
        return false;
    };

    // ============================================================
    // 6. CALCULATE ALLOWED WINDOW WITH TOLERANCE
    // ============================================================

    // Add the indicator's configured tolerance.
    //
    // Example:
    // tolerance = 0.021
    // expected = 24 hours
    // allowed = approximately 24 hours + 30 minutes.
    let allowed_duration_ms =
        (expected_duration.num_milliseconds() as f64
            * (1.0 + tolerance)) as i64;

    // ============================================================
    // 7. CANDLE FRESHNESS VALIDATION
    // ============================================================

    // Convert the newest candle's end time to Unix milliseconds.
    let last_end_ms =
        last.end.timestamp_millis();

    // Maximum age = twice the candle timeframe.
    let max_candle_age_ms =
        candle_duration.num_milliseconds() * 2;

    // Reject candles that end after the current market-event time.
    //
    // Also reject candles older than twice their timeframe.
    //
    // Examples for ONE_MINUTE:
    // 1-minute-old candle = ACCEPT.
    // 2-minute-old candle = ACCEPT.
    // 5-minute-old candle = REJECT.
    if last_end_ms > current_ts
        || current_ts.saturating_sub(last_end_ms) > max_candle_age_ms
    {
        return false;
    }

    // ============================================================
    // 8. HISTORICAL WINDOW VALIDATION
    // ============================================================

    // Convert the oldest candle's start time to Unix milliseconds.
    let first_start_ms =
        first.start.timestamp_millis();

    // Measure the total time covered by the selected candles.
    //
    // This includes any missing intervals between candles.
    let actual_duration_ms =
        last_end_ms.saturating_sub(first_start_ms);

    // Reject windows extending beyond the configured tolerance.
    //
    // Example:
    // Expected = 24 hours.
    // Allowed = approximately 24h 30m.
    //
    // Actual = 24h 10m => ACCEPT.
    // Actual = 24h 45m => REJECT.
    if actual_duration_ms > allowed_duration_ms {
        return false;
    }

    // ============================================================
    // 9. VALIDATION SUCCESSFUL
    // ============================================================

    // All enabled safeguards passed.
    true
}