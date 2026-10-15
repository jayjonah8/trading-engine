#![allow(dead_code)]

/// Execution type of an order.
///
/// `OrderType` describes how an order becomes executable at a venue.
///
/// Venue-specific order types should not be added here unless they represent
/// a generally useful execution concept. Exchange adapters are responsible
/// for translating these canonical types into venue-specific API requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderType {
    /// Executes against available liquidity as soon as possible.
    Market,

    /// Executes only at the specified limit price or better.
    Limit,

    /// Becomes a market order once the stop price is triggered.
    StopMarket,

    /// Becomes a limit order once the stop price is triggered.
    StopLimit,
}

impl OrderType {
    // ============================================================
    // ESTIMATE MAKER / TAKER (BACKTEST / SIMULATION)
    // ============================================================

    /// Estimates maker/taker classification based on OrderType.
    ///
    /// Intended for simplified Backtest and Simulation fee models.
    ///
    /// Market      -> Taker
    /// Limit       -> Maker (assumed)
    /// StopMarket  -> Taker
    /// StopLimit   -> Maker (assumed)
    ///
    /// NOTE:
    /// Limit orders can execute as takers when crossing the spread.
    /// This simplified model assumes limit orders provide liquidity.
    ///
    /// Live/Paper adapters should use venue-reported execution fees.
    ///
    /// This implementation is equivalent to:
    ///
    /// match order_type {
    ///     OrderType::Market => false,
    ///     OrderType::Limit => true,
    ///     OrderType::StopMarket => false,
    ///     OrderType::StopLimit => true,
    /// }
    ///
    /// `matches!` provides a shorter implementation with identical behavior.
    pub const fn is_maker(&self) -> bool {
        matches!(
            self,
            Self::Limit | Self::StopLimit
        )
    }
}