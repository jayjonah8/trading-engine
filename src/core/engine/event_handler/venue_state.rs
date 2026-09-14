#![allow(dead_code)]

use crate::core::{
    engine::engine::Engine,
    venue_state::VenueStateEvent,
};

use crate::shared::print_log;


// ============================================================
// HANDLE VENUE STATE EVENT
// ============================================================

impl Engine {
    /// Applies canonical external venue-state updates.
    ///
    /// Venue-specific pollers translate their raw API responses before
    /// reaching this handler.
    ///
    /// The Engine remains the sole authoritative owner of `VenueState`.
    pub fn on_venue_state_event(
        &mut self,
        event: VenueStateEvent,
    ) {
        match event {
            // ========================================================
            // ACCOUNT
            // ========================================================

            VenueStateEvent::AccountUpdated {
                account,
            } => {
                let venue =
                    account.venue;

                let account_id =
                    account.account_id.clone();

                self.venue_state
                    .insert_account(
                        account
                    );

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] VENUE ACCOUNT UPDATED | venue={:?} | account_id={}",
                        venue,
                        account_id,
                    ),
                );
            }


            // ========================================================
            // INSTRUMENT
            // ========================================================

            VenueStateEvent::InstrumentsUpdated {
                venue,
                instruments,
            } => {
                let instrument_count =
                    instruments.len();

                self.venue_state
                    .replace_instruments_for_venue(
                        venue,
                        instruments,
                    );

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] VENUE INSTRUMENTS UPDATED | venue={:?} | instruments={}",
                        venue,
                        instrument_count,
                    ),
                );
            }
        }

        // Publish after every venue-state update so EngineView stays 
        // synchronized with authoritative Engine state.
        self.publish_view();
    }
}