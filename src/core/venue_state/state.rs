#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::{
    execution::Venue,

    venue_state::{
        AccountState,
        InstrumentKey,
        InstrumentState,
        VenueAccountKey,
    },
};


// ============================================================
// VENUE STATE
// ============================================================

/// Authoritative Engine-owned state describing external venue/account and
/// instrument information.
///
/// This state is separate from EngineState because it describes the external
/// trading environment rather than canonical execution lifecycle state.
///
/// EngineState:
///
///     Orders
///     Trades
///     Positions
///     Fills
///     Risk
///
/// VenueState:
///
///     Accounts
///     Instrument metadata
#[derive(Debug, Clone, Default)]
pub struct VenueState {
    pub accounts:
        HashMap<
            VenueAccountKey,
            AccountState,
        >,

    pub instruments:
        HashMap<
            InstrumentKey,
            InstrumentState,
        >,
}

impl VenueState {
    pub fn new() -> Self {
        Self::default()
    }


    // ============================================================
    // ACCOUNTS
    // ============================================================

    pub fn insert_account(
        &mut self,
        account: AccountState,
    ) -> Option<AccountState> {
        let key =
            account.key();

        self.accounts.insert(
            key,
            account,
        )
    }


    pub fn account(
        &self,
        venue: Venue,
        account_id: &str,
    ) -> Option<&AccountState> {
        let key =
            VenueAccountKey::new(
                venue,
                account_id,
            );

        self.accounts.get(
            &key
        )
    }


    pub fn account_mut(
        &mut self,
        venue: Venue,
        account_id: &str,
    ) -> Option<&mut AccountState> {
        let key =
            VenueAccountKey::new(
                venue,
                account_id,
            );

        self.accounts.get_mut(
            &key
        )
    }


    // ============================================================
    // INSTRUMENTS
    // ============================================================

    pub fn insert_instrument(
        &mut self,
        instrument: InstrumentState,
    ) -> Option<InstrumentState> {
        let key =
            instrument.key();

        self.instruments.insert(
            key,
            instrument,
        )
    }


    pub fn instrument(
        &self,
        venue: Venue,
        symbol: &str,
    ) -> Option<&InstrumentState> {
        let key =
            InstrumentKey::new(
                venue,
                symbol,
            );

        self.instruments.get(
            &key
        )
    }


    pub fn instrument_mut(
        &mut self,
        venue: Venue,
        symbol: &str,
    ) -> Option<&mut InstrumentState> {
        let key =
            InstrumentKey::new(
                venue,
                symbol,
            );

        self.instruments.get_mut(
            &key
        )
    }
}