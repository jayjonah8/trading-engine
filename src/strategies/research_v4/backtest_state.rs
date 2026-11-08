#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::{
    core::{
        engine::engine::EngineEvent,
        engine_view::EngineView,
    },

    market::candle_history::CandleHistory,

    toolbox::{
        TradeCooldown,
        TradeDurationWatchdog,
        black_swan_protection::BlackSwanProtection,
    },
};


// ============================================================
// TRENDING CONFIGURATION
// ============================================================

pub struct TrendingConfig {
    pub enabled: bool,

    // Rolling observation window used to identify trends.
    pub trend_window_secs: u64,

    // Maximum allowed pullback duration.
    pub max_pullback_secs: u64,

    // Minimum trend movement required.
    pub min_trend_move_pct: f64,

    // Minimum pullback depth before confirmation.
    pub min_pullback_pct: f64,

    // Required recovery from pullback extreme.
    pub confirmation_pct: f64,

    // Enable confirmation before entering.
    pub enable_confirmations: bool,

    // Profit and loss thresholds.
    pub take_profit_pct: Option<f64>,
    pub stop_loss_pct: Option<f64>,

    // Maximum trade duration.
    pub trade_duration_secs: Option<u64>,
}


// ============================================================
// MEAN REVERSION CONFIGURATION
// ============================================================

pub struct MeanReversionConfig {
    pub enabled: bool,

    // Two rolling observation windows used to validate ranges.
    pub short_window_secs: u64,
    pub long_window_secs: u64,

    // Minimum and maximum accepted range width.
    pub min_range_width_pct: f64,
    pub max_range_width_pct: f64,

    // Maximum allowed difference between range boundaries
    // across the two observation windows.
    pub max_range_boundary_diff_pct: f64,

    // Percentage of the total range width used to define
    // proximity to the upper and lower boundaries.
    // Example: 20.0 = outer 20% of each side of the range.
    pub boundary_proximity_pct: f64,

    // Minimum number of confirmed alternating boundary reversals.
    pub min_confirmed_reversals: usize,

    // Minimum reversal distance as a fraction of the established range width.
    pub min_reversal_fraction: f64,

    // Required reversal from the observed extreme.
    pub confirmation_pct: f64,

    // Enable confirmation before entering.
    pub enable_confirmations: bool,

    // Fraction of the established range used for take profit.
    // Example: 0.75 = 75% of the range.
    pub range_profit_target_fraction: f64,

    // Maximum permitted movement beyond a range boundary.
    pub boundary_break_buffer_pct: f64,

    // Optional stop loss.
    pub stop_loss_pct: Option<f64>,

    // Maximum trade duration.
    pub trade_duration_secs: Option<u64>,
}


// ============================================================
// MEAN REVERSION BOUNDARY STATE
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MeanReversionBoundary {
    Lower,
    Upper,
}

#[derive(Clone, Debug)]
pub struct MeanReversionBoundaryMonitor {

    // Boundary responsible for the potential entry.
    pub boundary: MeanReversionBoundary,

    // Timestamp when the setup was first detected.
    pub detected_ts_ms: i64,

    // Price when the setup was first detected.
    pub detected_price: f64,

    // Lowest price for LONG setups.
    // Highest price for SHORT setups.
    pub extreme_price: f64,

    // Prevents repeated confirmations from the same setup.
    pub confirmed: bool,

    // Prevent duplicate trade submissions.
    pub trade_submitted: bool,
}


// ============================================================
// MEAN REVERSION RANGE STATE
// ============================================================

#[derive(Clone, Debug)]
pub struct MeanReversionRange {

    // Established upper price boundary of the trading range.
    // Represents potential resistance for short entries.
    pub upper_boundary: f64,

    // Established lower price boundary of the trading range.
    // Represents potential support for long entries.
    pub lower_boundary: f64,

    // Percentage distance between the upper and lower boundaries.
    // Example: $100 low and $106 high = 6% range width.
    pub range_width_pct: f64,

    // Highest price observed within the shorter rolling window.
    pub short_range_high: f64,

    // Lowest price observed within the shorter rolling window.
    pub short_range_low: f64,

    // Highest price observed within the longer rolling window.
    pub long_range_high: f64,

    // Lowest price observed within the longer rolling window.
    pub long_range_low: f64,

    // Timestamp when the range was first validated.
    // This remains unchanged while the range is active.
    pub detected_ts_ms: i64,
}


// ============================================================
// RESEARCH V4 BACKTEST STRATEGY
// ============================================================

pub struct ResearchV4BacktestStrategy {
    // ========================================================
    // GENERAL
    // ========================================================

    pub enable_strategy: bool,
    pub debug_mode: bool,
    pub(crate) tx_engine: UnboundedSender<EngineEvent>,
    pub(crate) rx_view: watch::Receiver<Arc<EngineView>>,

    // ========================================================
    // CAPITAL / EXECUTION
    // ========================================================

    pub max_trades_allowed: usize,
    pub reference_margin_usd: f64,
    pub trade_amount_usd: f64,
    pub leverage: Option<f64>,

    // Emergency stop loss.
    pub emergency_exit_percent: Option<f64>,

    // Minimum delay before normal stop loss activates.
    pub min_sl_delay: Option<u64>,

    // ========================================================
    // MARKET QUALIFICATION
    // ========================================================

    // Master switch for trading eligibility checks.
    pub enable_trading_eligibility_check: bool,

    // Minimum USD trading volume over the last 24 hours.
    // None = disable restriction.
    pub min_volume_usd: Option<f64>,

    // Minimum absolute 24-hour price movement.
    // None = disable restriction.
    pub min_24h_move_pct: Option<f64>,

    // Maximum absolute 24-hour price movement.
    // None = disable restriction.
    pub max_24h_move_pct: Option<f64>,

    // Minimum recent relative volume.
    // Example: 0.75 = 75% of average recent volume.
    // None = disable restriction.
    pub min_relative_volume: Option<f64>,

    // Number of completed one-minute candles used for RVOL.
    // Example: 20 = compare the latest minute against the
    // preceding 20 one-minute candles.
    pub relative_volume_period: usize,

    // Cached eligibility per symbol.
    pub can_trade: HashMap<String, bool>,

    // ========================================================
    // TREND FOLLOWING CONFIGURATIONS
    // ========================================================

    // Supports multiple independent trend configurations.
    pub trending_configs: Vec<TrendingConfig>,

    // ========================================================
    // MEAN REVERSION CONFIGURATIONS
    // ========================================================

    // Supports multiple independent range configurations.
    pub mean_reversion_configs: Vec<MeanReversionConfig>,

    // Cached validated trading ranges for each symbol.
    // Boundaries remain fixed until invalidation or expiration.
    // TODO: Track by (symbol, config_id) to support multiple
    // independent configurations per asset.
    pub mean_reversion_ranges: HashMap<String, MeanReversionRange>,

    // Tracks tick-level price approaches to preserved boundaries.
    // Lower boundary = potential long setup.
    // Upper boundary = potential short setup.
    // TODO: Track by (symbol, config_id) when supporting
    // multiple independent configurations per asset.
    pub mean_reversion_boundary_monitors:
        HashMap<String, MeanReversionBoundaryMonitor>,

    // ========================================================
    // MARKET DATA
    // ========================================================

    // Stores completed multi-timeframe candle history.
    pub candle_history: CandleHistory,

    // ========================================================
    // RISK / TOOLBOX
    // ========================================================

    pub black_swan_protection: BlackSwanProtection,
    pub trade_cooldown: TradeCooldown,
    pub trade_duration_watchdog: TradeDurationWatchdog,
}