#![allow(dead_code)]

#[derive(Debug, Default, Clone)]
pub struct Config {
   
    /// List of market symbols/assets the bot is allowed to trade.
    pub symbols: Vec<String>,

    /// Maximum number of positions the bot can have open at the same time.
    pub max_gross_positions: usize,

    /// Minimum number of seconds a trade must remain open before it can be exited.
    pub min_trade_age_before_exit_secs: u64,

    /// Enables simulation/paper-trading logic instead of real trading.
    pub enable_simulation_mode: bool,

    /// Uses historical market data instead of the live Alpaca feed.
    pub enable_backtest_mode: bool,

    /// Starting account balance used when running a historical backtest.
    ///
    /// Backtesting does not query Alpaca for account information, so this
    /// value is used to initialize the fake backtest account balance.
    pub backtest_starting_balance: f64,

    /// Enable/disable Discord remote notifications.
    pub enable_discord_notifications: bool,

    /// Enable/disable Slack remote notifications.
    pub enable_slack_notifications: bool,
}