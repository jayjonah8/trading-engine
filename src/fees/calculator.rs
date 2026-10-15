#![allow(dead_code)]

use crate::{
    core::execution::Venue,

    fees::venues::hyperliquid::HyperliquidFees,
};


// ============================================================
// CALCULATE EXECUTION FEE
// ============================================================

/// Calculates an estimated execution fee for a specific venue.
///
/// Intended for Backtest and Simulation adapters.
///
/// Live/Paper adapters should use venue-reported fees when available.
///
/// V1:
/// - Hyperliquid maker/taker fees
/// - Other venues temporarily return zero
///
/// TODO:
/// Add Alpaca and Interactive Brokers fee models.
///
/// TODO:
/// Move venue-specific fee rates into Config.
///
/// TODO:
/// Support volume-tiered fee schedules.
pub fn calculate_execution_fee(
    venue: Venue,
    quantity: f64,
    price: f64,
    is_maker: bool,
) -> f64 {
    match venue {
        Venue::Hyperliquid => {
            let fees = HyperliquidFees::new(
                0.00015,
                0.00045,
            );

            fees.calculate(
                quantity,
                price,
                is_maker,
            )
        }

        Venue::Alpaca => {
            // TODO:
            // Implement Alpaca commissions and applicable fees.
            0.0
        }

        Venue::InteractiveBrokers => {
            // TODO:
            // Implement IBKR commissions and applicable fees.
            0.0
        }
    }
}