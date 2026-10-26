#![allow(dead_code)]

use crate::{
    core::{
        execution::Venue,
        engine::state::ExecutionMode,
    },
    data::{
        historical::provider::HistoricalDataProvider,
        live::provider::LiveMarketDataProvider,
    },
};

#[derive(Debug, Clone)]
pub struct Config {
    // ============================================================
    // EXECUTION / MARKET DATA
    // ============================================================

    /// Determines how orders are executed by the engine.
    ///
    /// Live and Paper route orders to a real venue adapter, while
    /// Simulation and Backtest use internal simulated execution.
    pub execution_mode: ExecutionMode,

    /// Default execution venue used by the engine.
    ///
    /// The venue remains relevant in all execution modes because
    /// venue-specific behavior such as fees may still apply.
    pub execution_venue: Venue,

    /// Market-data provider used while running in live mode.
    ///
    /// This determines where streaming Quote, TapeTrade, and other live
    /// market-data events originate.
    pub live_market_data_provider: LiveMarketDataProvider,

    /// Historical-data provider used while running backtests.
    ///
    /// This is independent from the live market-data provider so historical
    /// data sources can be selected specifically for backtesting.
    pub historical_data_provider: HistoricalDataProvider,

    /// List of market symbols/assets the bot is allowed to trade.
    pub symbols: Vec<String>,

    // ============================================================
    // TRADE / ASSET LIMITS
    // ============================================================

    /// Maximum number of simultaneously active directional Trades
    /// across the entire Engine.
    ///
    /// `Opening`, `Open`, and `Closing` trades count toward this limit.
    ///
    /// `None` disables this limit.
    pub max_active_trades: Option<usize>,

    /// Maximum number of distinct symbols that may have active Trades
    /// at the same time.
    ///
    /// Example:
    ///
    /// AAPL: 2 active trades
    /// MSFT: 1 active trade
    /// NVDA: 3 active trades
    ///
    /// active assets = 3
    ///
    /// `None` disables this limit.
    pub max_active_assets: Option<usize>,

    /// Maximum number of simultaneously active Trades allowed for one symbol.
    ///
    /// This is engine-wide and may include Trades from multiple strategies.
    ///
    /// `None` disables this limit.
    pub max_active_trades_per_symbol: Option<usize>,

    /// Maximum number of simultaneously active Trades allowed for one strategy
    /// across all symbols.
    ///
    /// `None` disables this limit.
    pub max_active_trades_per_strategy: Option<usize>,

    // ============================================================
    // NOTIONAL LIMITS
    // ============================================================

    /// Maximum notional value allowed for a single submitted order.
    ///
    /// `None` disables this limit.
    pub max_order_notional: Option<f64>,

    /// Maximum gross notional exposure allowed across the Engine.
    ///
    /// Gross exposure uses absolute exposure regardless of long/short direction.
    ///
    /// `None` disables this limit.
    pub max_gross_notional: Option<f64>,

    /// Maximum gross notional exposure allowed for a single symbol.
    ///
    /// `None` disables this limit.
    pub max_symbol_notional: Option<f64>,

    // ============================================================
    // ENGINE SETTINGS
    // ============================================================

    // Flatten all live trades when starting the bot for the selected venue
    pub close_trades_on_startup: bool,

    /// Starting account balance used when running simulation or backtests.
    pub custom_starting_balance: f64,

    /// Enable/disable Discord remote notifications.
    pub enable_discord_notifications: bool,

    /// Enable/disable Slack remote notifications.
    pub enable_slack_notifications: bool,
}