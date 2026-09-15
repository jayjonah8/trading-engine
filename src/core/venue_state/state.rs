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


    /// Replaces the complete instrument snapshot for one venue.
    ///
    /// Removing the old snapshot first ensures assets removed from venue
    /// metadata do not remain incorrectly tradable.
    pub fn replace_instruments_for_venue(
        &mut self,
        venue: Venue,
        instruments: Vec<InstrumentState>,
    ) {
        self.instruments
            .retain(
                |key, _| {
                    key.venue != venue
                }
            );


        for instrument in instruments {
            self.insert_instrument(
                instrument
            );
        }
    }


    // ============================================================
    // QUANTITY NORMALIZATION
    // ============================================================

    /// Normalizes a requested quantity using canonical instrument metadata.
    ///
    /// Venue-specific metadata such as Hyperliquid `szDecimals` must be
    /// translated into InstrumentState before reaching the Engine.
    ///
    /// This keeps Engine order handling venue-independent.
    pub fn normalize_quantity(
        &self,
        venue: Venue,
        symbol: &str,
        raw_quantity: f64,
    ) -> Result<f64, String> {
        // ========================================================
        // VALIDATE RAW QUANTITY
        // ========================================================

        if !raw_quantity.is_finite()
            || raw_quantity <= 0.0
        {
            return Err(
                format!(
                    "invalid raw quantity | venue={:?} | symbol={} | raw_quantity={}",
                    venue,
                    symbol,
                    raw_quantity,
                )
            );
        }


        // ========================================================
        // INSTRUMENT METADATA
        // ========================================================

        // Missing canonical metadata means the Engine cannot safely determine
        // the venue's execution rules, so the order must be rejected.
        let instrument =
            self.instrument(
                venue,
                symbol,
            )
            .ok_or_else(
                || {
                    format!(
                        "instrument metadata unavailable | venue={:?} | symbol={}",
                        venue,
                        symbol,
                    )
                }
            )?;


        // ========================================================
        // TRADING STATUS
        // ========================================================

        // Metadata may exist for an instrument that is not currently tradable.
        if !instrument.active {
            return Err(
                format!(
                    "instrument is not active | venue={:?} | symbol={}",
                    venue,
                    symbol,
                )
            );
        }


        // ========================================================
        // NORMALIZE QUANTITY
        // ========================================================

        let quantity =
            if let Some(step) =
                instrument.quantity_step
            {
                // ====================================================
                // EXPLICIT QUANTITY STEP
                // ====================================================

                if !step.is_finite()
                    || step <= 0.0
                {
                    return Err(
                        format!(
                            "invalid quantity step | venue={:?} | symbol={} | step={}",
                            venue,
                            symbol,
                            step,
                        )
                    );
                }


                // Floor so normalization never increases requested exposure.
                (raw_quantity / step)
                    .floor()
                    * step
            }

            else if let Some(decimals) =
                instrument.quantity_decimals
            {
                // ====================================================
                // DECIMAL PRECISION
                // ====================================================

                let factor =
                    10_f64.powi(
                        decimals as i32
                    );


                // Floor so normalization never increases requested exposure.
                (raw_quantity * factor)
                    .floor()
                    / factor
            }

            else if instrument.fractional_quantity_supported {
                // ====================================================
                // UNRESTRICTED FRACTIONAL QUANTITY
                // ====================================================

                raw_quantity
            }

            else {
                // ====================================================
                // WHOLE UNITS ONLY
                // ====================================================

                raw_quantity.floor()
            };


        // ========================================================
        // VALIDATE NORMALIZED QUANTITY
        // ========================================================

        if !quantity.is_finite()
            || quantity <= 0.0
        {
            return Err(
                format!(
                    "normalized quantity is zero or invalid | venue={:?} | symbol={} | raw_quantity={} | normalized_quantity={}",
                    venue,
                    symbol,
                    raw_quantity,
                    quantity,
                )
            );
        }


        // ========================================================
        // MINIMUM QUANTITY
        // ========================================================

        if let Some(min_quantity) =
            instrument.min_quantity
        {
            if quantity < min_quantity {
                return Err(
                    format!(
                        "quantity below minimum | venue={:?} | symbol={} | quantity={} | min_quantity={}",
                        venue,
                        symbol,
                        quantity,
                        min_quantity,
                    )
                );
            }
        }


        Ok(
            quantity
        )
    }
}