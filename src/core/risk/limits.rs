#![allow(dead_code)]

/// Configured limits used by the engine's risk system.
///
/// `RiskLimits` contains policy values only. It does not inspect engine state
/// or decide whether an order should be accepted.
///
/// Risk decisions are performed by the risk check layer using these limits
/// together with current positions, trades, orders, and reservations.
///
/// `None` means that a particular limit is disabled.
///
/// TODO: Replace temporary primitive monetary values with canonical strongly
/// typed `Money` / `Quantity` types where appropriate.
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// Maximum number of active logical Trades across the entire Engine.
    ///
    /// Example:
    ///
    /// BTC Trade 1
    /// BTC Trade 2
    /// ETH Trade 3
    /// SOL Trade 4
    ///
    /// Active Trades = 4
    pub max_active_trades: Option<usize>,

    /// Maximum number of distinct symbols/assets that may have active Trades.
    ///
    /// Example:
    ///
    /// BTC Trade 1
    /// BTC Trade 2
    /// ETH Trade 3
    ///
    /// Active Assets = 2 (BTC + ETH)
    pub max_active_assets: Option<usize>,

    /// Maximum number of active Trades allowed for one symbol.
    ///
    /// Example:
    ///
    /// BTC Trade 1
    /// BTC Trade 2
    ///
    /// If the limit is 2, a third BTC Trade would be rejected.
    pub max_active_trades_per_symbol: Option<usize>,

    /// Maximum number of active Trades belonging to one strategy
    /// across all symbols.
    ///
    /// Example:
    ///
    /// BaseStrategy -> BTC
    /// BaseStrategy -> ETH
    /// BaseStrategy -> SOL
    ///
    /// If the limit is 3, a fourth BaseStrategy Trade would be rejected.
    pub max_active_trades_per_strategy: Option<usize>,

    /// Maximum notional value allowed for one individual Order.
    ///
    /// Example with a $10,000 limit:
    ///
    /// $8,000 Order  -> allowed
    /// $10,000 Order -> allowed
    /// $15,000 Order -> rejected
    pub max_order_notional: Option<f64>,

    /// Maximum total gross notional exposure across the entire Engine.
    ///
    /// Long and short exposure are both counted by absolute value.
    ///
    /// Example:
    ///
    /// BTC long  = $20,000
    /// ETH long  = $15,000
    /// SOL short = $10,000
    ///
    /// Gross Notional = $45,000
    pub max_gross_notional: Option<f64>,

    /// Maximum gross notional exposure allowed for one symbol.
    ///
    /// Exposure from multiple Trades on the same symbol is combined.
    ///
    /// Example:
    ///
    /// BTC Trade 1 = $10,000
    /// BTC Trade 2 = $8,000
    ///
    /// BTC Symbol Notional = $18,000
    ///
    /// With a $20,000 limit, another $5,000 of BTC exposure
    /// would be rejected.
    pub max_symbol_notional: Option<f64>,
}

impl RiskLimits {
    pub fn new() -> Self {
        Self {
            max_active_trades: None,
            max_active_assets: None,
            max_active_trades_per_symbol: None,
            max_active_trades_per_strategy: None,
            max_order_notional: None,
            max_gross_notional: None,
            max_symbol_notional: None,
        }
    }
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self::new()
    }
}