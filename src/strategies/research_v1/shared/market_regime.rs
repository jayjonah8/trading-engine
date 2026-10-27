use crate::indicators::{
    MarketBreadth,
    MarketVolumeAcceleration,
    MarketVolumeExpansion,
};


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


// ============================================================
// MARKET REGIME MANAGER
// ============================================================

/// Determines the current market regime using three
/// synchronized market-wide indicators.
///
/// Trending requires:
/// - Sufficient market-wide volume expansion.
/// - Sufficient market-wide volume acceleration.
/// - Sufficient directional market breadth.
///
/// All three conditions must be satisfied.
///
/// Otherwise, MeanReversion is selected.
///
/// Returns None if the indicator snapshots are not synchronized.
///
/// No confirmation periods, cooldowns, or additional state.
pub fn market_regime_manager(
    expansion: &MarketVolumeExpansion,
    acceleration: &MarketVolumeAcceleration,
    breadth: &MarketBreadth,

    // Minimum percentage of assets experiencing unusually
    // high relative trading volume.
    //
    // Example: 50.0 means at least 50% of analyzed assets
    // must exceed the RVOL threshold configured in
    // market_volume_expansion().
    expansion_threshold_pct: f64,

    // Minimum percentage of assets experiencing increasing
    // relative trading volume compared to the previous candle.
    //
    // Example: 60.0 means at least 60% of analyzed assets
    // must have increasing RVOL.
    acceleration_threshold_pct: f64,

    // Minimum percentage of assets moving in the same direction.
    //
    // Example: 65.0 means at least 65% of analyzed assets
    // must be bullish OR at least 65% must be bearish.
    breadth_threshold_pct: f64,

) -> Option<MarketRegime> {

    // ====================================================
    // VERIFY SYNCHRONIZATION
    // ====================================================

    if expansion.snapshot_end != acceleration.snapshot_end
        || expansion.snapshot_end != breadth.snapshot_end
    {
        return None;
    }


    // ====================================================
    // MARKET VOLUME EXPANSION
    // ====================================================

    let volume_expanding =
        expansion.expanding_assets_pct
            >= expansion_threshold_pct;


    // ====================================================
    // MARKET VOLUME ACCELERATION
    // ====================================================

    let volume_accelerating =
        acceleration.accelerating_assets_pct
            >= acceleration_threshold_pct;


    // ====================================================
    // MARKET BREADTH
    // ====================================================

    let directional_breadth =
        breadth.bullish_assets_pct
            >= breadth_threshold_pct
        || breadth.bearish_assets_pct
            >= breadth_threshold_pct;


    // ====================================================
    // DETERMINE MARKET REGIME
    // ====================================================

    let trending =
        volume_expanding
        && volume_accelerating
        && directional_breadth;


    Some(
        if trending {
            MarketRegime::Trending
        } else {
            MarketRegime::MeanReversion
        }
    )
}