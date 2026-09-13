#![allow(dead_code)]

/// Requested changes to an existing canonical order.
///
/// Order replacement/modification intentionally supports only a conservative
/// set of execution parameters that are safe and portable across venues.
///
/// Canonical order identity remains immutable.
///
/// Immutable identity includes:
///
/// - OrderId
/// - venue
/// - strategy_id
/// - trade_id
/// - symbol
/// - side
/// - purpose
///
/// Even if a venue supports modifying broader fields such as side, symbol,
/// reduce-only behavior, or order type, the core engine does not expose those
/// changes through `OrderChanges`.
///
/// This keeps replacement behavior predictable across Alpaca, IBKR,
/// Hyperliquid, and future execution venues.
///
/// Currently replaceable:
///
/// - quantity
/// - limit_px
/// - stop_px
///
/// TODO:
/// Consider adding other broadly portable execution parameters later, such as:
///
/// - TimeInForce
/// - post_only
/// - reduce_only
///
/// only if their replacement semantics remain safe across supported venues.
///
/// TODO:
/// Replace temporary `f64` values with canonical `Quantity` and `Price` types.
#[derive(Debug, Clone, Default)]
pub struct OrderChanges {
    /// New TOTAL requested quantity for the order.
    ///
    /// This represents total order quantity, not remaining quantity.
    ///
    /// Example:
    ///
    /// Original quantity = 10
    /// Already filled     = 2
    /// Replacement        = 6
    ///
    /// New remaining quantity becomes:
    ///
    /// 6 - 2 = 4
    pub quantity: Option<f64>,

    /// New limit price when applicable.
    pub limit_px: Option<f64>,

    /// New stop trigger price when applicable.
    pub stop_px: Option<f64>,
}

impl OrderChanges {
    /// Returns true when no actual replacement fields were requested.
    pub fn is_empty(&self) -> bool {
        self.quantity.is_none()
            && self.limit_px.is_none()
            && self.stop_px.is_none()
    }
}