#![allow(dead_code)]

// ============================================================
// MARKET REGIME
// ============================================================

/// Defines the market regime used to determine trade direction.
///
/// MeanReversion:
///     Trade against price deviations.
///
/// Trending:
///     Trade in the direction of price deviations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    MeanReversion,
    Trending,
}