#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::{
    execution::Venue,
    venue::{
        AccountState,
        FractionalQuantityMode,
        InstrumentKey,
        InstrumentState,
        VenueAccountKey,
        VenueCapabilities,
    },
};


// open_trade.rs / close_trade.rs
//             │
//             ▼
//        VenueState
//             │
//      ┌──────┴──────┐
//      ▼             ▼
// VenueCapabilities  InstrumentState
// (always exists)    optional/required by venue
//      │             │
//      └──────┬──────┘
//             ▼
//  normalize + validate
//             │
//             ▼
//      canonical Order


// ============================================================================
// VENUE STATE
// ============================================================================
//
// VenueState contains Engine-owned information describing the external
// execution environment.
//
// It is intentionally separate from EngineState:
//
//     EngineState
//         -> Orders
//         -> Trades
//         -> Positions
//         -> Fills
//         -> Risk
//
//     VenueState
//         -> Accounts
//         -> Instrument metadata
//
// Order normalization combines:
//
//     VenueCapabilities
//         -> venue-wide/default execution rules
//
//     InstrumentState
//         -> optional per-instrument execution rules
//
// Instrument metadata policy:
//
//     Alpaca
//         Metadata is optional. VenueCapabilities provides safe defaults.
//
//     Interactive Brokers
//         Metadata is optional. VenueCapabilities provides safe defaults.
//
//     Hyperliquid
//         Metadata is required because executable quantity and price
//         precision depend on instrument-specific metadata.
//
// When InstrumentState exists, its explicit constraints take precedence over
// venue defaults. This allows richer venue metadata to be added later without
// changing Engine order construction logic.

#[derive(Debug, Clone, Default)]
pub struct VenueState {
    pub accounts: HashMap<VenueAccountKey, AccountState>,
    pub instruments: HashMap<InstrumentKey, InstrumentState>,
}

impl VenueState {
    pub fn new() -> Self {
        Self::default()
    }


    // =========================================================================
    // ACCOUNTS
    // =========================================================================

    pub fn insert_account(&mut self, account: AccountState) -> Option<AccountState> {
        let key = account.key();
        self.accounts.insert(key, account)
    }

    pub fn account(&self, venue: Venue, account_id: &str) -> Option<&AccountState> {
        let key = VenueAccountKey::new(venue, account_id);
        self.accounts.get(&key)
    }

    pub fn account_mut(&mut self, venue: Venue, account_id: &str) -> Option<&mut AccountState> {
        let key = VenueAccountKey::new(venue, account_id);
        self.accounts.get_mut(&key)
    }


    // =========================================================================
    // INSTRUMENTS
    // =========================================================================

    pub fn insert_instrument(&mut self, instrument: InstrumentState) -> Option<InstrumentState> {
        let key = instrument.key();
        self.instruments.insert(key, instrument)
    }

    pub fn instrument(&self, venue: Venue, symbol: &str) -> Option<&InstrumentState> {
        let key = InstrumentKey::new(venue, symbol);
        self.instruments.get(&key)
    }

    pub fn instrument_mut(&mut self, venue: Venue, symbol: &str) -> Option<&mut InstrumentState> {
        let key = InstrumentKey::new(venue, symbol);
        self.instruments.get_mut(&key)
    }

    /// Replaces the complete instrument snapshot for one venue.
    ///
    /// Removing the old snapshot first ensures instruments removed from venue
    /// metadata do not remain incorrectly represented by stale state.
    pub fn replace_instruments_for_venue(&mut self, venue: Venue, instruments: Vec<InstrumentState>) {
        self.instruments.retain(|key, _| key.venue != venue);

        for instrument in instruments {
            self.insert_instrument(instrument);
        }
    }


    // =========================================================================
    // INSTRUMENT VALIDATION
    // =========================================================================

    /// Returns available InstrumentState while enforcing the venue's metadata
    /// requirement.
    ///
    /// Alpaca and IBKR may return `Ok(None)` because their current execution
    /// paths have safe venue-level defaults.
    ///
    /// Hyperliquid requires metadata and therefore rejects `None`.
    fn instrument_for_execution(&self, venue: Venue, symbol: &str) -> Result<Option<&InstrumentState>, String> {
        let capabilities = VenueCapabilities::for_venue(venue);
        let instrument = self.instrument(venue, symbol);

        if capabilities.requires_instrument_metadata && instrument.is_none() {
            return Err(format!("instrument metadata unavailable | venue={:?} | symbol={}", venue, symbol));
        }

        if let Some(instrument) = instrument {
            if !instrument.active {
                return Err(format!("instrument is not active | venue={:?} | symbol={}", venue, symbol));
            }
        }

        Ok(instrument)
    }


    // =========================================================================
    // QUANTITY NORMALIZATION
    // =========================================================================

    /// Normalizes a desired quantity into an executable venue quantity.
    ///
    /// Strategies may size positions from target notional:
    ///
    ///     target_notional / price = desired quantity
    ///
    /// The execution layer then converts that desired quantity into the
    /// largest valid venue quantity without increasing requested exposure.
    ///
    /// When fractional quantities are disabled, the quantity is normalized
    /// directly to whole units.
    ///
    /// When fractional quantities are supported, instrument-specific
    /// constraints take precedence over venue defaults.
    ///
    /// The returned quantity is the exact quantity that should be stored in
    /// canonical Order.quantity and sent to the execution adapter.
    pub fn normalize_quantity(&self, venue: Venue, symbol: &str, raw_quantity: f64) -> Result<f64, String> {
        // =====================================================================
        // VALIDATE RAW QUANTITY
        // =====================================================================

        if !raw_quantity.is_finite() || raw_quantity <= 0.0 {
            return Err(format!(
                "invalid raw quantity | venue={:?} | symbol={} | raw_quantity={}",
                venue, symbol, raw_quantity,
            ));
        }


        // =====================================================================
        // RESOLVE EXECUTION RULES
        // =====================================================================

        let capabilities = VenueCapabilities::for_venue(venue);
        let instrument = self.instrument_for_execution(venue, symbol)?;

        // Determine whether fractional quantities are currently allowed.
        //
        // Disabled   -> always use whole units.
        // LongOnly   -> fractional support is enabled for now; Side + OrderPurpose
        //               will be added later to distinguish long vs short execution.
        // Instrument -> use the instrument's venue-specific fractional capability.
        let fractional_quantity_supported = match capabilities.fractional_quantity_mode {
            FractionalQuantityMode::Disabled => false,
            FractionalQuantityMode::LongOnly => true,
            FractionalQuantityMode::Instrument => instrument.map(|instrument| instrument.fractional_quantity_supported).unwrap_or(false),
        };

        // Instrument-specific precision takes precedence over venue defaults.
        let quantity_step = instrument
            .and_then(|instrument| instrument.quantity_step)
            .or(capabilities.default_quantity_step);

        let quantity_decimals = instrument
            .and_then(|instrument| instrument.quantity_decimals)
            .or(capabilities.default_quantity_decimals);


        // =====================================================================
        // NORMALIZE QUANTITY
        // =====================================================================

        // Fractional support is checked before decimal/step precision.
        //
        // This is important because a venue may still define fractional
        // precision defaults while fractional execution is currently disabled.
        //
        // Example:
        //
        //     raw quantity = 3.7284
        //
        //     fractional disabled -> 3
        //     fractional enabled  -> apply quantity step/decimal precision
        //
        // Flooring whole-unit quantities allows strategies to size from target
        // notional without requiring venue-specific quantity calculations.
        let quantity = if !fractional_quantity_supported {
            raw_quantity.floor()
        } else if let Some(step) = quantity_step {
            if !step.is_finite() || step <= 0.0 {
                return Err(format!(
                    "invalid quantity step | venue={:?} | symbol={} | step={}",
                    venue, symbol, step,
                ));
            }

            // Never increase requested exposure during quantity normalization.
            (raw_quantity / step).floor() * step
        } else if let Some(decimals) = quantity_decimals {
            let factor = 10_f64.powi(decimals as i32);

            // Never increase requested exposure during quantity normalization.
            (raw_quantity * factor).floor() / factor
        } else {
            raw_quantity
        };


        // =====================================================================
        // VALIDATE NORMALIZED QUANTITY
        // =====================================================================

        if !quantity.is_finite() || quantity <= 0.0 {
            return Err(format!(
                "normalized quantity is zero or invalid | venue={:?} | symbol={} | raw_quantity={} | normalized_quantity={}",
                venue, symbol, raw_quantity, quantity,
            ));
        }


        // =====================================================================
        // MINIMUM QUANTITY
        // =====================================================================

        if let Some(min_quantity) = instrument.and_then(|instrument| instrument.min_quantity) {
            if quantity < min_quantity {
                return Err(format!(
                    "quantity below minimum | venue={:?} | symbol={} | quantity={} | min_quantity={}",
                    venue, symbol, quantity, min_quantity,
                ));
            }
        }

        Ok(quantity)
    }


    // =========================================================================
    // PRICE NORMALIZATION
    // =========================================================================

    /// Normalizes a requested order price into an executable venue price.
    ///
    /// Supported constraints:
    ///
    /// - significant-digit precision
    /// - decimal precision
    /// - explicit price tick
    ///
    /// Instrument-specific constraints take precedence over venue defaults.
    ///
    /// Hyperliquid continues to require InstrumentState metadata. Alpaca and
    /// IBKR currently use their venue defaults when metadata is unavailable.
    pub fn normalize_price(&self, venue: Venue, symbol: &str, raw_price: f64) -> Result<f64, String> {
        // =====================================================================
        // VALIDATE RAW PRICE
        // =====================================================================

        if !raw_price.is_finite() || raw_price <= 0.0 {
            return Err(format!(
                "invalid raw price | venue={:?} | symbol={} | raw_price={}",
                venue, symbol, raw_price,
            ));
        }


        // =====================================================================
        // RESOLVE EXECUTION RULES
        // =====================================================================

        let capabilities = VenueCapabilities::for_venue(venue);
        let instrument = self.instrument_for_execution(venue, symbol)?;

        let significant_digits = instrument.and_then(|instrument| instrument.price_significant_digits);

        let price_decimals = instrument
            .and_then(|instrument| instrument.price_decimals)
            .or(capabilities.default_price_decimals);

        let price_tick = instrument
            .and_then(|instrument| instrument.price_tick)
            .or(capabilities.default_price_tick);

        let mut price = raw_price;


        // =====================================================================
        // SIGNIFICANT DIGITS
        // =====================================================================

        if let Some(significant_digits) = significant_digits {
            if significant_digits == 0 {
                return Err(format!(
                    "invalid price significant digits | venue={:?} | symbol={} | significant_digits={}",
                    venue, symbol, significant_digits,
                ));
            }

            let magnitude = price.abs().log10().floor();
            let scale_exponent = significant_digits as i32 - 1 - magnitude as i32;
            let factor = 10_f64.powi(scale_exponent);

            price = (price * factor).round() / factor;
        }


        // =====================================================================
        // DECIMAL PRECISION
        // =====================================================================

        if let Some(decimals) = price_decimals {
            let factor = 10_f64.powi(decimals as i32);
            price = (price * factor).round() / factor;
        }


        // =====================================================================
        // EXPLICIT PRICE TICK
        // =====================================================================

        if let Some(tick) = price_tick {
            if !tick.is_finite() || tick <= 0.0 {
                return Err(format!(
                    "invalid price tick | venue={:?} | symbol={} | tick={}",
                    venue, symbol, tick,
                ));
            }

            price = (price / tick).round() * tick;
        }


        // =====================================================================
        // VALIDATE NORMALIZED PRICE
        // =====================================================================

        if !price.is_finite() || price <= 0.0 {
            return Err(format!(
                "normalized price is zero or invalid | venue={:?} | symbol={} | raw_price={} | normalized_price={}",
                venue, symbol, raw_price, price,
            ));
        }

        Ok(price)
    }
}