#![allow(dead_code)]

use crate::core::execution::Venue;


// ============================================================
// INSTRUMENT KEY
// ============================================================

/// Uniquely identifies an instrument at a venue.
///
/// The same symbol may have different execution rules at different venues,
/// therefore Venue is part of the key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstrumentKey {
    pub venue: Venue,

    pub symbol: String,
}

impl InstrumentKey {
    pub fn new(
        venue: Venue,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            venue,
            symbol: symbol.into(),
        }
    }
}


// ============================================================
// INSTRUMENT STATE
// ============================================================

/// Canonical execution metadata for one venue instrument.
///
/// Venue-specific API metadata should be mapped into this structure before
/// reaching the Engine.
#[derive(Debug, Clone)]
pub struct InstrumentState {
    pub venue: Venue,

    pub symbol: String,

    // ========================================================
    // QUANTITY RULES
    // ========================================================

    /// Quantity decimal precision when the venue expresses quantity
    /// constraints in decimal places.
    ///
    /// Example:
    /// Hyperliquid `szDecimals`.
    pub quantity_decimals: Option<u32>,

    /// Explicit quantity increment when available.
    ///
    /// Example:
    /// 0.001
    pub quantity_step: Option<f64>,

    /// Whether fractional quantities are supported.
    pub fractional_quantity_supported: bool,

    // ========================================================
    // PRICE RULES
    // ========================================================

    /// Maximum decimal places allowed for price when the venue expresses
    /// price precision as decimal places.
    pub price_decimals: Option<u32>,

    /// Explicit minimum price increment when available.
    ///
    /// Example:
    /// $0.01
    pub price_tick: Option<f64>,

    /// Maximum number of significant digits allowed in an order price.
    ///
    /// This is separate from decimal-place precision because some venues,
    /// including Hyperliquid, constrain prices using significant figures in
    /// addition to decimal-place rules.
    pub price_significant_digits: Option<u32>,

    // ========================================================
    // MINIMUMS
    // ========================================================

    pub min_quantity: Option<f64>,

    pub min_notional: Option<f64>,

    // ========================================================
    // TRADING STATUS
    // ========================================================

    /// Whether the venue currently considers the instrument available for
    /// trading.
    pub active: bool,

    // ========================================================
    // UPDATE STATE
    // ========================================================

    pub updated_ts_ms: i64,
}

impl InstrumentState {
    pub fn key(
        &self,
    ) -> InstrumentKey {
        InstrumentKey::new(
            self.venue,
            self.symbol.clone(),
        )
    }
}