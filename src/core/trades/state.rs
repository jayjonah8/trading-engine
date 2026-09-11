#![allow(dead_code)]

/// Lifecycle state of a strategy trade.
///
/// `TradeState` represents the logical strategy-level lifecycle of a trade,
/// not the state of any individual execution order.
///
/// A trade may have multiple associated orders, each with its own `OrderState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeState {
    /// The trade has been created and an entry is being worked, but the
    /// intended position has not yet been fully established.
    /// The actual entry order still lives in Orders; 
    /// Opening simply describes where the logical Trade is in its lifecycle.
    Opening,

    /// The trade has established exposure and is actively open.
    Open,

    /// An exit is being worked but exposure still remains.
    ///
    /// The trade remains active until confirmed fills reduce its remaining
    /// quantity to zero.
    Closing,

    /// The trade no longer has any remaining exposure.
    Closed,
}

impl TradeState {
    /// Returns true when the trade has reached its terminal lifecycle state.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Returns true while the trade still participates in the engine's
    /// active trade lifecycle.
    pub const fn is_active(self) -> bool {
        !self.is_terminal()
    }
}