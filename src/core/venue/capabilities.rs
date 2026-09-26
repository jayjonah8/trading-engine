#![allow(dead_code)]

use crate::core::execution::Venue;


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
// VENUE EXECUTION CAPABILITIES
// ============================================================================
//
// This module defines the Engine's default execution rules for each supported
// venue.
//
// These rules describe execution behavior that is known even when canonical
// InstrumentState metadata is not available.
//
// Instrument-specific metadata remains separate in VenueState:
//
//     VenueCapabilities
//         -> default / venue-wide execution rules
//
//     InstrumentState
//         -> optional per-instrument execution metadata
//
// The normalization layer will combine both when constructing executable
// orders.
//
// Metadata policy:
//
//     Alpaca
//         Instrument metadata is optional.
//         Safe venue defaults allow orders to operate without it.
//
//     Interactive Brokers
//         Instrument metadata is optional.
//         Safe venue defaults allow orders to operate without it.
//
//     Hyperliquid
//         Instrument metadata is required because quantity and price precision
//         depend on the instrument's venue metadata.
//
// Venue-specific rules must remain centralized here rather than being spread
// through open_trade.rs, close_trade.rs, or individual strategies.


// ============================================================================
// FRACTIONAL QUANTITY MODE
// ============================================================================

/// Describes the Engine's current fractional quantity policy for a venue.
///
/// Disabled:
///     Fractional quantities are not currently used by the Engine.
///
/// LongOnly:
///     Reserved for venue workflows where fractional long-side execution is
///     supported but fractional short entry is not.
///
/// Instrument:
///     Fractional behavior is determined by InstrumentState metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FractionalQuantityMode {
    Disabled,
    LongOnly,
    Instrument,
}


// ============================================================================
// VENUE CAPABILITIES
// ============================================================================

/// Default execution capabilities for one venue.
///
/// These values are intentionally venue-level defaults. More specific
/// InstrumentState metadata may override or strengthen applicable quantity and
/// price constraints when metadata is available.
///
/// Hyperliquid requires InstrumentState metadata and therefore does not provide
/// fallback quantity or price precision here.
#[derive(Debug, Clone, Copy)]
pub struct VenueCapabilities {
    /// Whether an order must have canonical InstrumentState metadata before it
    /// can be normalized and submitted.
    pub requires_instrument_metadata: bool,

    /// Default fractional quantity behavior.
    pub fractional_quantity_mode: FractionalQuantityMode,

    /// Default quantity decimal precision when instrument metadata does not
    /// provide a more specific rule.
    pub default_quantity_decimals: Option<u32>,

    /// Default quantity increment when instrument metadata does not provide a
    /// more specific rule.
    pub default_quantity_step: Option<f64>,

    /// Default price decimal precision when instrument metadata does not
    /// provide a more specific rule.
    pub default_price_decimals: Option<u32>,

    /// Default price increment when instrument metadata does not provide a
    /// more specific rule.
    pub default_price_tick: Option<f64>,
}


// ============================================================================
// DEFAULT VENUE RULES
// ============================================================================

impl VenueCapabilities {
    /// Returns the Engine's default execution capabilities for a venue.
    ///
    /// These defaults preserve the execution behavior currently proven by the
    /// live venue tests while allowing richer InstrumentState metadata to be
    /// introduced later.
    pub const fn for_venue(venue: Venue) -> Self {
        match venue {
            // ================================================================
            // ALPACA
            // ================================================================
            //
            // Current Engine behavior:
            //
            // - instrument metadata is not required
            // - fractional long entries are supported
            // - fractional established-position exits are supported
            // - fractional short entries are not supported and fall back to whole shares
            // - fractional quantities are normalized to 9 decimal places
            // - tested U.S. equity limit prices use cent precision
            //
            // Alpaca fractional orders also require DAY time-in-force. That rule is
            // handled separately by the order capability/TIF normalization layer.
            Venue::Alpaca => Self {
                requires_instrument_metadata: false,
                fractional_quantity_mode: FractionalQuantityMode::LongOnly,
                default_quantity_decimals: Some(9),
                default_quantity_step: None,
                default_price_decimals: Some(2),
                default_price_tick: Some(0.01),
            },

            // ================================================================
            // INTERACTIVE BROKERS
            // ================================================================
            //
            // Current Engine/API behavior:
            //
            // - instrument metadata is not required
            // - quantities are normalized to whole shares
            // - tested U.S. equity limit prices use cent precision
            //
            // Richer IBKR contract metadata may later replace these fallback
            // rules on a per-instrument basis.
            Venue::InteractiveBrokers => Self {
                requires_instrument_metadata: false,
                fractional_quantity_mode: FractionalQuantityMode::Disabled,
                default_quantity_decimals: None,
                default_quantity_step: Some(1.0),
                default_price_decimals: Some(2),
                default_price_tick: Some(0.01),
            },

            // ================================================================
            // HYPERLIQUID
            // ================================================================
            //
            // Hyperliquid instrument metadata is mandatory because executable
            // quantity and price precision depend on the instrument.
            //
            // No fallback precision is intentionally provided.
            Venue::Hyperliquid => Self {
                requires_instrument_metadata: true,
                fractional_quantity_mode: FractionalQuantityMode::Instrument,
                default_quantity_decimals: None,
                default_quantity_step: None,
                default_price_decimals: None,
                default_price_tick: None,
            },
        }
    }
}