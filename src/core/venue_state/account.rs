#![allow(dead_code)]

use crate::core::{
    execution::Venue,

    venue_state::{
        AlpacaAccountState,
        HyperliquidAccountState,
        IbkrAccountState,
    },
};

// Generic Engine logic
//         ↓
// AccountState
//         ↓
// equity
// cash
// available_balance
// buying_power
// margin_used
//
// Venue-specific logic
//         ↓
// AccountState.details
//         ↓
// Hyperliquid(...)
// Alpaca(...)
// InteractiveBrokers(...)


// ============================================================
// VENUE ACCOUNT KEY
// ============================================================

/// Uniquely identifies one account at one venue.
///
/// `account_id` is intentionally opaque to the Engine.
///
/// Examples:
///
/// Hyperliquid:
///     wallet / account address
///
/// Alpaca:
///     account ID
///
/// IBKR:
///     account code
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VenueAccountKey {
    pub venue: Venue,
    pub account_id: String,
}

impl VenueAccountKey {
    pub fn new(
        venue: Venue,
        account_id: impl Into<String>,
    ) -> Self {
        Self {
            venue,
            account_id: account_id.into(),
        }
    }
}


// ============================================================
// ACCOUNT STATE
// ============================================================

/// Canonical account state available to the Engine.
///
/// Fields at this level represent concepts that may be useful to generic
/// Engine/risk logic.
///
/// A field is optional because not every venue exposes every concept with
/// identical semantics.
///
/// Venue-specific information belongs in `details`.
#[derive(Debug, Clone)]
pub struct AccountState {
    pub venue: Venue,

    pub account_id: String,

    // ========================================================
    // CANONICAL ACCOUNT VALUES
    // ========================================================

    /// Total account equity / net liquidation value when available.
    pub equity: Option<f64>,

    /// Cash balance when the venue exposes a meaningful cash value.
    pub cash: Option<f64>,

    /// Capital currently available for new exposure.
    pub available_balance: Option<f64>,

    /// Buying power when explicitly exposed by the venue.
    pub buying_power: Option<f64>,

    /// Margin currently committed/used.
    pub margin_used: Option<f64>,

    // ========================================================
    // UPDATE STATE
    // ========================================================

    pub updated_ts_ms: i64,

    // ========================================================
    // VENUE-SPECIFIC DETAILS
    // ========================================================

    pub details: AccountDetails,
}

impl AccountState {
    pub fn key(
        &self,
    ) -> VenueAccountKey {
        VenueAccountKey::new(
            self.venue,
            self.account_id.clone(),
        )
    }
}


// ============================================================
// ACCOUNT DETAILS
// ============================================================

/// Venue-specific account values.
///
/// Generic Engine logic should normally use the canonical fields on
/// `AccountState`.
///
/// Venue-specific components may inspect this enum when they genuinely need
/// information that does not have a clean cross-venue meaning.
#[derive(Debug, Clone)]
pub enum AccountDetails {
    Hyperliquid(
        HyperliquidAccountState
    ),

    Alpaca(
        AlpacaAccountState
    ),

    InteractiveBrokers(
        IbkrAccountState
    ),
}