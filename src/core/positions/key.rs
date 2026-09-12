#![allow(dead_code)]

use crate::core::execution::Venue;

/// Unique key identifying an actual venue position.
///
/// Positions are keyed by venue, account, and symbol rather than symbol alone
/// because the engine may eventually trade the same instrument through
/// multiple venues or accounts simultaneously.
///
/// For example:
///
/// `Hyperliquid / account_1 / HYPE`
///
/// and
///
/// `Hyperliquid / account_2 / HYPE`
///
/// are two separate positions.
///
/// TODO: Replace temporary `String` account/symbol values with canonical
/// `AccountId` and `Symbol` types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    /// Execution venue where the position exists.
    pub venue: Venue,

    /// Venue account that owns the position.
    ///
    /// TODO: Replace with `AccountId`.
    pub account_id: String,

    /// Instrument represented by this position.
    ///
    /// TODO: Replace with canonical `Symbol`.
    pub symbol: String,
}

impl PositionKey {
    /// Creates a new position key.
    pub fn new(
        venue: Venue,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            venue,
            account_id: account_id.into(),
            symbol: symbol.into(),
        }
    }
}