#![allow(dead_code)]
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
/// TODO: Replace temporary `String` values with canonical strongly typed
/// `Venue`, `AccountId`, and `Symbol` types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    /// Execution venue where the position exists.
    ///
    /// TODO: Replace with `Venue`.
    pub venue: String,

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
        venue: impl Into<String>,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            venue: venue.into(),
            account_id: account_id.into(),
            symbol: symbol.into(),
        }
    }
}