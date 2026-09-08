use std::fmt;

/// Internal engine identifier for an order.
///
/// This ID is owned by the trading engine and is independent of any
/// exchange/broker order ID.
///
/// A `u64` is intentionally used instead of a UUID because internal order IDs
/// only need to be unique within the engine's identity domain. A `u64` is
/// smaller, faster to copy, compare, and hash, and avoids unnecessary UUID
/// generation overhead in high-throughput order processing.
///
/// Venue-specific IDs (Alpaca, IBKR, Hyperliquid, etc.) are stored separately
/// on the `Order`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OrderId(u64);

impl OrderId {
    /// Creates an `OrderId` from its raw numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying numeric ID.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}