use crate::market::timeframe::Timeframe;


// ====================================================
// PRICE LEVELS CONFIGURATION
// ====================================================
//
// Defines the configuration settings used by the Price
// Levels toolbox to identify significant support and
// resistance zones across different market assets.
//
// Configuration includes:
// - Candle timeframe used for price level analysis.
// - Historical lookback duration in seconds.
// - Swing strength for identifying local highs and lows.
// - Percentage tolerance for clustering nearby price levels.
// - Minimum independent price reactions required.
// - Maximum number of significant levels returned.
//
// The historical lookback is automatically converted into
// the appropriate number of candles for the selected timeframe.
//
// These settings allow trading strategies to customize how
// price levels are detected without modifying the underlying
// detection algorithms.
//
// TODO: Support multiple timeframe configurations for
// combined support and resistance analysis.


// ====================================================
// PRICE LEVEL DETECTOR CONFIG
// ====================================================

#[derive(Debug, Clone)]
pub struct PriceLevelConfig {
    pub enabled: bool,

    // Candle timeframe used for detecting price levels.
    pub timeframe: Timeframe,

    // Historical lookback duration.
    pub lookback_secs: u64,

    // Number of candles on each side of a swing.
    pub swing_strength: usize,

    // Maximum percentage distance for clustering levels.
    pub cluster_tolerance_pct: f64,

    // Minimum independent swing reactions required.
    pub min_reactions: usize,

    // Maximum number of significant levels returned.
    pub max_levels: usize,
}

impl Default for PriceLevelConfig {
    fn default() -> Self {
        Self {
            enabled: true,

            timeframe: Timeframe::FIVE_MINUTES,

            lookback_secs: 24 * 60 * 60, // 24 hours

            swing_strength: 3,

            cluster_tolerance_pct: 0.25,

            min_reactions: 2,

            max_levels: 20,
        }
    }
}

impl PriceLevelConfig {

    // ====================================================
    // REQUIRED CANDLE COUNT
    // ====================================================

    pub fn required_candles(&self) -> usize {
        let timeframe_secs = self.timeframe.as_secs();

        if timeframe_secs == 0 {
            return 0;
        }

        self.lookback_secs
            .div_ceil(timeframe_secs)
            .min(usize::MAX as u64) as usize
    }
}