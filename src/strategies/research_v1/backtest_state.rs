#![allow(dead_code)]

use std::sync::Arc;
use std::collections::HashMap;

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

    toolbox::MarketRegime,
    toolbox::MultiSync,
    toolbox::price_window_ts::PriceWindowTs,
    toolbox::TradeCooldown,
    toolbox::TradeDurationWatchdog,
    toolbox::black_swan_protection::BlackSwanProtection,
    toolbox::early_profit_taking::{
        EarlyProfitTaking,
    },
    toolbox::price_levels::{
        PriceLevelConfig,
        PriceLevelDetector,
    },
};


pub struct ResearchV1BacktestStrategy {
    // Master switch to enable or disable the strategy.
    pub enable_strategy: bool,

    // Master switch to enable or disable strategy debug logging.
    pub debug_mode: bool,

    // Channel used to send strategy events and trade requests to the engine.
    pub(crate) tx_engine: UnboundedSender<EngineEvent>,

    // Receiver providing the latest shared engine state.
    pub(crate) rx_view: watch::Receiver<Arc<EngineView>>,

    /// Maximum number of simultaneous open trades allowed for this strategy.
    pub max_trades_allowed: usize,

    /// Strategy-level capital reference used to calculate Multi-Sync PnL percentages.
    /// Can be updated during runtime to support automatic compounding.
    pub reference_margin_usd: f64,

    // Amount of USD allocated to each trade before leverage (e.g. 100.0 = $100)
    pub trade_amount_usd: f64,

    /// None means no strategy-level leverage adjustment.
    pub leverage: Option<f64>,

    // Minimum percentage price movement required to trigger a trade entry.
    pub entry_percent: Option<f64>,

    // Profit percentage at which the position should be closed (e.g. 3.0 = 3%).
    pub exit_profit_percent: Option<f64>,

    // Loss percentage at which the position should be closed.
    pub exit_loss_percent: Option<f64>,

    // Emergency stop-loss.
    pub emergency_exit_percent: Option<f64>,

    // Duration in seconds to delay hard stop-loss.
    pub min_sl_delay: Option<u64>,

    // Time limit in seconds until an open trade is auto closed.
    pub trade_duration_secs: Option<u64>,

    // Minimum USD trading volume required to allow a trade entry.
    // For a rolling-volume filter, accumulate this value over a defined time window rather than indefinitely.
    pub min_volume_usd: Option<f64>,    

    // Maximum allowed 24-hour price movement percentage.
    // None = no limit.
    pub max_24h_move_pct: Option<f64>,

    // Current market regime determining trade direction.
    pub market_regime: MarketRegime,

    /// Tracks strategy-specific realized PnL and manages
    /// temporary regime reversals through Profit Protection
    /// and Drawdown Protection.
    pub multi_sync: MultiSync,

    // true  = keep tracking the symbol's rolling price window while a trade is open.
    // false = clear that symbol's rolling window and stop tracking it until the trade is closed.
    pub track_price_window_while_in_trade: bool,

    // Timestamp-based rolling price window.
    pub price_window: PriceWindowTs,

    // Timestamp-based btc rolling price window.
    pub btc_window: PriceWindowTs,

    // Blocks trades during a btc vacuum
    pub btc_vacuum_protection_enabled: bool,

    // Threshold percent that determines a btc vacuum
    pub btc_vacuum_threshold_pct: f64,

    pub btc_vacuum_active: bool,

    // Closed multi-timeframe candle history.
    pub candle_history: CandleHistory,

    // Detects support and resistance price levels
    pub price_levels: PriceLevelDetector,

    /// Enables or disables can_trade trading eligibility checks.
    /// When enabled, trades must pass the cached symbol eligibility check.
    /// When disabled, all symbols are considered eligible for trading.
    pub enable_trading_eligibility_check: bool,

    // Cached trading eligibility for each symbol (24hr volume checks etc).
    // Updated whenever a completed one-minute candle arrives.
    pub can_trade: HashMap<String, bool>,

    // black swan event protection (halts trading during extreme moves across all assets)
    pub black_swan_protection: BlackSwanProtection,

    // Tracks per-symbol trade entry cooldowns.
    pub trade_cooldown: TradeCooldown,

    // Closes only this strategies trades that are past their trade duration that have not been closed already.
    // Acts as a backup to the engine's normal trade close logic.
    pub trade_duration_watchdog: TradeDurationWatchdog,

    /// Strategy-specific early-profit-taking configuration.
    pub early_profit_taking: EarlyProfitTaking,
}