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
    /// Maximum number of simultaneously active directional trades.
    ///
    /// `Opening`, `Open`, and `Closing` trades may all count toward this
    /// limit depending on the risk check being performed.
    pub max_active_trades: Option<usize>,

    /// Maximum notional value allowed for a single submitted order.
    ///
    /// TODO: Replace with `Money`.
    pub max_order_notional: Option<f64>,

    /// Maximum gross notional exposure allowed across the engine.
    ///
    /// Gross exposure considers absolute exposure regardless of direction.
    ///
    /// TODO: Replace with `Money`.
    pub max_gross_notional: Option<f64>,

    /// Maximum gross notional exposure allowed for a single symbol.
    ///
    /// TODO: Replace with `Money`.
    pub max_symbol_notional: Option<f64>,
}

impl RiskLimits {
    pub fn new() -> Self {
        Self {
            max_active_trades: None,
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