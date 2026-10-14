#![allow(dead_code)]

/// Hyperliquid maker/taker fee configuration.
///
/// Rates are expressed as decimal fractions:
/// - 0.00015 = 0.015%
/// - 0.00045 = 0.045%
///
/// Fees are calculated using the actual execution notional.
#[derive(Debug, Clone, Copy)]
pub struct HyperliquidFees {
    pub maker_rate: f64,
    pub taker_rate: f64,
}

impl HyperliquidFees {
    /// Creates a new Hyperliquid fee configuration.
    pub fn new(
        maker_rate: f64,
        taker_rate: f64,
    ) -> Self {
        Self {
            maker_rate,
            taker_rate,
        }
    }

    /// Calculates the fee for a single execution fill.
    ///
    /// Works for:
    /// - Market orders
    /// - Limit orders
    /// - Partial fills
    /// - Opening fills
    /// - Closing fills
    ///
    /// `is_maker` should reflect how the order actually executed.
    pub fn calculate(
        &self,
        quantity: f64,
        price: f64,
        is_maker: bool,
    ) -> f64 {
        let notional = quantity.abs() * price;

        let rate = if is_maker {
            self.maker_rate
        } else {
            self.taker_rate
        };

        notional * rate
    }
}