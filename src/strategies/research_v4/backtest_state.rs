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
        weighted_trade_flip::WeightedTradeFlip,
    },
};


// ============================================================
// STRATEGY CONFIGURATION KEY
// ============================================================

// Identifies an independent strategy configuration
// operating on a specific asset.
//
// Allows multiple configurations to maintain their own
// ranges, boundary monitors, and entry signals.
//
// Example:
// BTC + mr_4h
// BTC + mr_8h
// ETH + mr_4h

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StrategyConfigKey {
    pub symbol: String,
    pub config_id: String,
}

impl StrategyConfigKey {
    pub fn new(
        symbol: &str,
        config_id: &str,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            config_id: config_id.to_string(),
        }
    }
}


// ============================================================
// MEAN REVERSION CONFIGURATION
// ============================================================

pub struct MeanReversionConfig {

    // Unique identifier for this configuration.
    pub id: String,

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
    pub boundary_proximity_pct: f64,

    // Minimum number of confirmed alternating boundary reversals.
    pub min_confirmed_reversals: usize,

    // Minimum reversal distance as a fraction of the established range width.
    pub min_reversal_fraction: f64,

    // Required reversal from the observed extreme.
    pub confirmation_pct: f64,

    // Enable confirmation before entering.
    pub enable_confirmations: bool,

    // Determines how reversal confirmation is evaluated.
    //
    // true  = Confirm using completed one-minute candle closes.
    // false = Confirm using individual trade ticks.
    //
    // Intrabar price extremes are still tracked using ticks,
    // regardless of the selected confirmation mode.

    pub require_candle_confirmation: bool,

    // Maximum permitted movement beyond a range boundary.
    // Cancels pending reversal setups on intrabar breakouts.
    pub boundary_break_buffer_pct: f64,

    // Independent risk configuration for this range setup.
    pub risk: MeanReversionRiskConfig,
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

    // Prevents repeated confirmations.
    pub confirmed: bool,

    // Prevents duplicate trade submissions.
    pub trade_submitted: bool,
}


// ============================================================
// MEAN REVERSION RANGE STATE
// ============================================================

#[derive(Clone, Debug)]
pub struct MeanReversionRange {

    // Established upper price boundary.
    pub upper_boundary: f64,

    // Established lower price boundary.
    pub lower_boundary: f64,

    // Percentage width of the validated range.
    pub range_width_pct: f64,

    // Short-window boundaries.
    pub short_range_high: f64,
    pub short_range_low: f64,

    // Long-window boundaries.
    pub long_range_high: f64,
    pub long_range_low: f64,

    // Timestamp when the range was validated.
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

    pub emergency_exit_percent: Option<f64>,
    pub min_sl_delay: Option<u64>,

    // ========================================================
    // MARKET QUALIFICATION
    // ========================================================

    pub enable_trading_eligibility_check: bool,

    pub min_volume_usd: Option<f64>,
    pub min_24h_move_pct: Option<f64>,
    pub max_24h_move_pct: Option<f64>,

    pub min_relative_volume: Option<f64>,
    pub relative_volume_period: usize,

    // Cached eligibility per asset.
    pub can_trade: HashMap<String, bool>,

    // ========================================================
    // MEAN REVERSION CONFIGURATIONS
    // ========================================================

    // Supports any number of independent configurations.
    pub mean_reversion_configs: Vec<MeanReversionConfig>,

    // Validated ranges indexed by symbol and configuration.
    pub mean_reversion_ranges:
        HashMap<StrategyConfigKey, MeanReversionRange>,

    // Independent boundary monitors for each configuration.
    pub mean_reversion_boundary_monitors:
        HashMap<StrategyConfigKey, MeanReversionBoundaryMonitor>,

    // ====================================================
    // BREAKOUT CONFIGURATIONS & STATE
    // ====================================================

    // Independent breakout configurations.
    pub breakout_configs: Vec<BreakoutConfig>,

    // Confirmed breakout setups tracked independently
    // by symbol, range configuration, and breakout configuration.
    pub breakout_setups:
        HashMap<BreakoutConfigKey, BreakoutSetup>,

    // ========================================================
    // MARKET DATA
    // ========================================================

    pub candle_history: CandleHistory,

    // ========================================================
    // RISK / TOOLBOX
    // ========================================================

    pub black_swan_protection: BlackSwanProtection,
    pub trade_cooldown: TradeCooldown,
    pub trade_duration_watchdog: TradeDurationWatchdog,

    // Probability-based trade direction reversal.
    pub weighted_trade_flip: WeightedTradeFlip,
}


// ============================================================
// MEAN REVERSION RISK CONFIGURATION
// ============================================================

pub struct MeanReversionRiskConfig {

    // Fraction of the established range used for take profit.
    //
    // Example:
    // 0.70 = target 70% of the validated range.
    pub range_profit_target_fraction: f64,

    // Percentage buffer beyond the validated range boundary
    // used to calculate the protective stop loss.
    //
    // LONG:
    // Stop loss below the lower boundary.
    //
    // SHORT:
    // Stop loss above the upper boundary.
    //
    // Example:
    // 0.5 = 0.5% beyond the boundary.
    pub stop_loss_boundary_buffer_pct: f64,

    // Maximum trade duration.
    pub trade_duration_secs: Option<u64>,
}


// ============================================================
// BREAKOUT RISK CONFIGURATION
// ============================================================

pub struct BreakoutRiskConfig {

    // Take-profit distance from the breakout entry price.
    //
    // Example:
    // 3.0 = 3% favorable price movement.
    pub take_profit_pct: f64,

    // Stop-loss distance from the breakout entry price.
    //
    // Example:
    // 2.0 = 2% adverse price movement.
    pub stop_loss_pct: f64,

    // Maximum trade duration.
    pub trade_duration_secs: Option<u64>,
}


// ============================================================
// BREAKOUT CONFIGURATION
// ============================================================

pub struct BreakoutConfig {

    // Unique identifier for this breakout configuration.
    pub id: String,

    // Master switch for breakout trading.
    pub enabled: bool,

    // Percentage movement beyond the validated range
    // boundary required to confirm a breakout.
    //
    // Example:
    // 0.5 = price must move 0.5% beyond the boundary.
    pub confirmation_pct: f64,

    // Require a completed one-minute candle to confirm
    // the breakout rather than relying on an intrabar tick.
    pub require_candle_confirmation: bool,

    // Independent risk settings for breakout trades.
    pub risk: BreakoutRiskConfig,

    // Maximum time a breakout setup can remain pending.
    //
    // Example:
    // 900 = 15 minutes.
    //
    // If the breakout has not confirmed within this period,
    // the setup is removed.
    pub setup_expiration_secs: u64,
}


// ============================================================
// BREAKOUT DIRECTION
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakoutDirection {
    Bullish,
    Bearish,
}


// ============================================================
// BREAKOUT SETUP STATE
// ============================================================

#[derive(Clone, Debug)]
pub struct BreakoutSetup {

    // Direction of the original range breakout.
    pub direction: BreakoutDirection,

    // Original validated range boundaries.
    pub upper_boundary: f64,
    pub lower_boundary: f64,

    // Price when the original range was invalidated.
    pub breakout_price: f64,

    // Timestamp when the original range was invalidated.
    pub breakout_ts_ms: i64,

    // Price when this configuration confirmed its breakout.
    pub confirmation_price: Option<f64>,

    // Timestamp when this configuration confirmed.
    pub confirmed_ts_ms: Option<i64>,

    // Whether this configuration has confirmed its breakout.
    pub confirmed: bool,

    // Prevent duplicate trade submissions.
    pub trade_submitted: bool,
}


// ============================================================
// BREAKOUT CONFIGURATION KEY
// ============================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BreakoutConfigKey {
    pub symbol: String,
    pub range_config_id: String,
    pub breakout_config_id: String,
}

impl BreakoutConfigKey {
    pub fn new(
        symbol: &str,
        range_config_id: &str,
        breakout_config_id: &str,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            range_config_id: range_config_id.to_string(),
            breakout_config_id: breakout_config_id.to_string(),
        }
    }
}