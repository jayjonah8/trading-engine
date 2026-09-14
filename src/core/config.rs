#![allow(dead_code)]

use crate::core::execution::Venue;

#[derive(Debug, Clone)]
pub struct Config {
    /// Default execution venue used when the engine creates live orders.
    pub execution_venue: Venue,

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
    // OTHER ENGINE SETTINGS
    // ============================================================

    // Maybe delete later.
    /// Minimum number of seconds a trade must remain open before it can be exited.
    pub min_trade_age_before_exit_secs: u64,

    /// Enables simulation/paper-trading logic instead of real trading.
    pub enable_simulation_mode: bool,

    /// Uses historical market data instead of the live feed.
    pub enable_backtest_mode: bool,

    /// Starting account balance used when running a historical backtest.
    pub backtest_starting_balance: f64,

    /// Enable/disable Discord remote notifications.
    pub enable_discord_notifications: bool,

    /// Enable/disable Slack remote notifications.
    pub enable_slack_notifications: bool,
}