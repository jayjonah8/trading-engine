pub mod config;
pub mod detector;
pub mod types;
pub mod range;
pub mod visualization;

#[cfg(test)]
mod tests;

pub use config::PriceLevelConfig;
pub use detector::PriceLevelDetector;

pub use types::{
    PriceLevel,
    PriceLevelType,
    PriceLevels,
};

pub use visualization::generate_price_levels_chart;

// ====================================================
// TODO: MULTI-TIMEFRAME CONFLUENCE
// ====================================================
//
// Combine significant levels detected across multiple
// candle timeframes.
//
// Potential enhancements:
// - Merge overlapping zones across timeframes.
// - Increase strength for multi-timeframe confluence.
// - Preserve originating timeframe information.
// - Weight higher-timeframe levels appropriately.
//
// ====================================================
// TODO: DYNAMIC MARKET STRUCTURE
// ====================================================
//
// Track additional level behavior:
//
// - Confirmed breakouts.
// - Support/resistance flips.
// - Retests.
// - Reclaims.
// - Level invalidation.
// - Historical level reliability.