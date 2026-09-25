#![allow(dead_code)]

use crate::core::{
    execution::Venue,
    venue::{
        AccountState,
        InstrumentState,
    },
};


// ============================================================
// VENUE STATE EVENT
// ============================================================

/// Canonical updates to Engine-owned external venue state.
///
/// Venue-specific pollers and runtime components map their raw API responses
/// into these canonical updates before sending them to the Engine.
///
/// Examples:
///
/// Hyperliquid Meta poller
///     -> InstrumentUpdated
///
/// Hyperliquid account poller
///     -> AccountUpdated
///
/// Alpaca account poller
///     -> AccountUpdated
///
/// IBKR account poller
///     -> AccountUpdated
///
/// Raw SDK/API response types must never cross this boundary.
#[derive(Debug, Clone)]
pub enum VenueStateEvent {
    AccountUpdated {
        account: AccountState,
    },

    /// Replaces/refreshes the canonical instrument snapshot for a venue.
    InstrumentsUpdated {
        venue: Venue,
        instruments: Vec<InstrumentState>,
    },
}
