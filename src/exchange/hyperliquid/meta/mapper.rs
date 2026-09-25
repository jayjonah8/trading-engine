#![allow(dead_code)]

use chrono::Utc;

use hyperliquid_rust_sdk::Meta;

use crate::core::{
    execution::Venue,
    venue::InstrumentState,
};


// ============================================================
// MAP HYPERLIQUID META
// ============================================================

/// Converts raw Hyperliquid Meta into canonical Engine instrument state.
///
/// Hyperliquid-specific SDK types remain outside core Engine state.
///
/// Hyperliquid Meta provides `szDecimals`, which directly defines quantity
/// precision and also participates in the venue's allowed price precision.
///
/// Hyperliquid prices additionally use a maximum significant-digit rule.
/// These venue-specific rules are translated here into canonical
/// InstrumentState metadata so the Engine itself remains venue-independent.
///
/// The resulting metadata can later be consumed by:
///
/// - VenueState::normalize_quantity()
/// - VenueState::normalize_price()
///
/// before a canonical Order reaches the execution adapter.
pub fn map_meta_to_instruments(
    meta: &Meta,
) -> Vec<InstrumentState> {
    let ts_ms =
        Utc::now()
            .timestamp_millis();


    meta.universe
        .iter()
        .map(
            |asset| {
                InstrumentState {
                    venue: Venue::Hyperliquid,
                    symbol: asset.name.clone(),

                    // ================================================
                    // QUANTITY RULES
                    // ================================================

                    /// Hyperliquid directly provides `szDecimals`.
                    quantity_decimals: Some(asset.sz_decimals),

                    /// Hyperliquid quantity precision is represented by
                    /// szDecimals rather than an explicit quantity step.
                    quantity_step: None,

                    fractional_quantity_supported:
                        asset.sz_decimals > 0,

                    // ================================================
                    // PRICE RULES
                    // ================================================

                    /// For perpetual assets, Hyperliquid allows prices with
                    /// up to `6 - szDecimals` decimal places, subject also
                    /// to the significant-digit constraint below.
                    price_decimals:
                        Some(
                            6_u32.saturating_sub(
                                asset.sz_decimals
                            )
                        ),

                    /// Hyperliquid does not expose the perpetual price rule
                    /// as a conventional fixed tick size in Meta.
                    price_tick: None,

                    /// Hyperliquid perpetual order prices are constrained to
                    /// at most five significant digits.
                    price_significant_digits: Some(5),

                    // ================================================
                    // MINIMUMS
                    // ================================================

                    min_quantity: None,
                    min_notional: None,

                    // ================================================
                    // TRADING STATUS
                    // ================================================

                    active: true,

                    // ================================================
                    // UPDATE STATE
                    // ================================================

                    updated_ts_ms: ts_ms,
                }
            }
        )
        .collect()
}