#![allow(dead_code)]

#[derive(Debug, Clone)]
pub struct TradeIntent {
    /// Name of the strategy generating this intent (e.g. "MeanReversion")
    pub strategy: String,

    /// Trading stock symbol (e.g. "AAPL")
    pub symbol: String,

    /// Direction of the trade: true = long, false = short
    pub is_long: bool,

    /// Current midprice at the moment the intent is generated
    pub price: f64,

    /// Amount to use for trade 
    pub trade_amount_usd: f64,

    // @todo delete entry_percent. The engine does not need it and some
    // strategies might never even use entry_percent to enter a trade at all

    /// Percent threshold hit to trigger entry (e.g. 3.2 meaning +3.2%)
    pub entry_percent: f64,

    // @todo make this exit_profit_percent a Option and update the engine. Strategies should
    // the option to exit by exit_profit_percent or any other indicator 
    // the want or not to use them at all.

    /// Desired take-profit percentage for exit
    pub exit_profit_percent: f64,

    // @todo make this exit_loss_percent a Option and update the engine. Strategies should
    // the option to exit by exit_loss_percent or any other indicator 
    // the want or not to use them at all.

    /// Percent stop-loss (normal SL)
    pub exit_loss_percent: f64,

    // @todo make emergency_exit_percent an Option and update the engine (see reasons above)

    /// Emergency stop-loss (hard SL if price collapses)
    pub emergency_exit_percent: f64,

    // @todo make min_sl_delay an Option and update the engine (see reasons above)

    /// Delay in seconds before stop-loss checks are enabled
    pub min_sl_delay: u64,

    /// Whether time-based auto-close is enabled
    pub enable_trade_duration: bool,

    /// Optional limit for how long a trade may stay open
    /// If Some(x), trade auto-closes after x seconds.
    pub trade_duration_secs: Option<u64>,

    /// Timestamp of intent in milliseconds
    pub ts_ms: i64,
}