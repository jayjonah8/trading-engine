#![allow(dead_code)]

use chrono::Utc;

use hyperliquid_rust_sdk::Meta;

use crate::core::{
    execution::Venue,
    venue_state::InstrumentState,
};


// ============================================================
// MAP HYPERLIQUID META
// ============================================================

/// Converts raw Hyperliquid Meta into canonical Engine instrument state.
///
/// Hyperliquid-specific SDK types remain outside core Engine state.
///
/// Currently Hyperliquid gives us:
///
///     name
///     szDecimals
///
/// Additional canonical instrument rules can be populated later as the
/// venue exposes or requires them.
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

                    symbol:
                        asset.name.clone(),

                    quantity_decimals:
                        Some(
                            asset.sz_decimals
                        ),

                    quantity_step:
                        None,

                    fractional_quantity_supported:
                        asset.sz_decimals > 0,

                    price_decimals:
                        None,

                    price_tick:
                        None,

                    min_quantity:
                        None,

                    min_notional:
                        None,

                    active:
                        true,

                    updated_ts_ms:
                        ts_ms,
                }
            }
        )
        .collect()
}