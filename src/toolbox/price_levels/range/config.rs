#![allow(dead_code)]

use crate::market::timeframe::Timeframe;


// ====================================================
// RANGE DETECTOR CONFIGURATION
// ====================================================
//
// Configuration for identifying established trading
// ranges using detected support/resistance levels
// and completed historical OHLCV candles.
//
// The detector uses a single configurable observation
// window and candle timeframe.
//
// Historical analysis is controlled by the timestamp
// supplied to RangeDetector::update(), allowing the
// same logic to operate on both historical and
// current market data.
//
// Future:
// - Support multiple timeframes simultaneously.
// - Detect confluence between ranges on different
//   timeframes.
// - Support multiple independent configurations.

#[derive(Debug, Clone)]
pub struct RangeConfig {

    // ====================================================
    // GENERAL
    // ====================================================

    pub enabled: bool,

    // Candle timeframe used for range detection.
    pub timeframe: Timeframe,

    // Historical observation window in seconds.
    pub lookback_secs: u64,

    // ====================================================
    // RANGE WIDTH
    // ====================================================

    // Minimum accepted range width as a percentage.
    pub min_range_width_pct: f64,

    // Maximum accepted range width as a percentage.
    pub max_range_width_pct: f64,

    // ====================================================
    // BOUNDARY VALIDATION
    // ====================================================

    // Minimum independent reactions required at
    // each support/resistance boundary.
    pub min_boundary_touches: usize,

    // Minimum completed movements between opposite
    // range boundaries.
    pub min_confirmed_oscillations: usize,

    // Minimum movement required to qualify as a
    // boundary-to-boundary oscillation.
    //
    // Expressed as a fraction of the range width.
    //
    // Example:
    // 0.70 = price must travel at least 70%
    // of the established range width.
    pub min_reversal_fraction: f64,

    // Boundary proximity expressed as a percentage
    // of the established range width.
    //
    // Example:
    // 20.0 = the outer 20% of the range near
    // each boundary qualifies as a boundary approach.
    pub boundary_proximity_pct: f64,

    // Maximum permitted candle-close breakout beyond
    // a range boundary before invalidation.
    //
    // Expressed as a percentage of the boundary price.
    //
    // Example:
    // 0.15 = a 0.15% breakout buffer.
    //
    // Intrabar wicks alone do not invalidate a range.
    pub boundary_break_buffer_pct: f64,

    // ====================================================
    // PRICE CONTAINMENT
    // ====================================================

    // Minimum percentage of completed candles that
    // must remain within the established range.
    //
    // Example:
    // 80.0 = at least 80% of candles.
    pub min_containment_pct: f64,

    // ====================================================
    // OUTPUT
    // ====================================================

    // Maximum number of validated ranges retained.
    pub max_ranges: usize,
}


// ====================================================
// DEFAULT CONFIGURATION
// ====================================================

impl Default for RangeConfig {

    fn default() -> Self {
        Self {
            enabled: true,

            timeframe: Timeframe::FIVE_MINUTES,

            lookback_secs: 24 * 60 * 60,

            min_range_width_pct: 0.5,
            max_range_width_pct: 5.0,

            min_boundary_touches: 2,
            min_confirmed_oscillations: 2,

            min_reversal_fraction: 0.70,

            boundary_proximity_pct: 20.0,
            boundary_break_buffer_pct: 0.15,

            min_containment_pct: 80.0,

            max_ranges: 10,
        }
    }
}