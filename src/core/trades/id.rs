#![allow(dead_code)]

use std::fmt;

// TradeId(17)                 Logical strategy trade
//    │
//    ├── OrderId(91)          Entry
//    ├── OrderId(103)         Stop loss
//    └── OrderId(118)         Exit
//
// Exchange oid/cid values won’t live directly on Trade anymore. 
// The associated Order objects own those execution identifiers.


/// Internal engine identifier for a strategy trade.
///
/// A `TradeId` identifies the logical lifecycle of a directional trade inside
/// the trading engine. It is separate from `OrderId` because a single trade
/// may create and own multiple orders throughout its lifetime.
///
/// For example, one trade may be associated with:
///
/// - an entry order
/// - multiple partial entry fills
/// - a take-profit order
/// - a stop-loss order
/// - one or more exit orders
///
/// `TradeId` is also intentionally independent of exchange/broker order IDs.
/// Venue-specific identifiers belong to the associated `Order` objects.
///
/// A `u64` is intentionally used instead of a UUID because trade IDs only
/// need to be unique within the engine's identity domain. A `u64` is smaller,
/// faster to copy, compare, and hash, and avoids unnecessary UUID generation
/// overhead in high-throughput engine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TradeId(u64);

impl TradeId {
    /// Creates a `TradeId` from its raw numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying numeric ID.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TradeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}