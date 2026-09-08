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