use crate::core::positions::key::PositionKey;

/// Canonical engine representation of an actual venue position.
///
/// A `Position` represents real exposure held at a venue/account after fills
/// have occurred. It is intentionally separate from `Trade`, which represents
/// a strategy's logical trading lifecycle.
///
/// Positions should change because of confirmed fills or explicit venue
/// reconciliation — not because a strategy merely requested an order.
///
/// A positive quantity represents net long exposure.
/// A negative quantity represents net short exposure.
/// Zero represents no exposure.
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `f64` quantity -> `Quantity`
/// - `f64` prices -> `Price`
/// - `f64` PnL -> `Money`
#[derive(Debug, Clone)]
pub struct Position {
    /// Venue/account/symbol identity of this position.
    pub key: PositionKey,

    /// Current signed net quantity.
    ///
    /// Positive = long
    /// Negative = short
    /// Zero = flat
    ///
    /// TODO: Replace with a canonical signed `Quantity` representation.
    pub quantity: f64,

    /// Average entry price of the currently open quantity.
    ///
    /// `None` when the position is flat.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub avg_entry_px: Option<f64>,

    /// Realized profit/loss accumulated from reductions and closes.
    ///
    /// TODO: Replace with `Money`.
    pub realized_pnl: f64,

    /// Most recent timestamp at which this position changed.
    pub updated_ts_ms: i64,
}

impl Position {
    /// Creates a new flat position.
    pub fn new(key: PositionKey, ts_ms: i64) -> Self {
        Self {
            key,
            quantity: 0.0,
            avg_entry_px: None,
            realized_pnl: 0.0,
            updated_ts_ms: ts_ms,
        }
    }

    /// Returns true when there is no current exposure.
    pub fn is_flat(&self) -> bool {
        self.quantity == 0.0
    }

    /// Returns true when the position has net long exposure.
    pub fn is_long(&self) -> bool {
        self.quantity > 0.0
    }

    /// Returns true when the position has net short exposure.
    pub fn is_short(&self) -> bool {
        self.quantity < 0.0
    }

    /// Returns the absolute quantity currently exposed.
    pub fn absolute_quantity(&self) -> f64 {
        self.quantity.abs()
    }
}