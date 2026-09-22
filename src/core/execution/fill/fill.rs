#![allow(dead_code)]

use crate::core::execution::{
    fill::id::ExecutionId,
    OrderId,
    Side,
    Venue,
};


/// Immutable record of an actual order execution.
///
/// A single `Order` may produce multiple fills at different quantities and
/// prices. Each fill represents an execution that actually occurred and
/// should never be modified after it has been recorded.
///
/// Fills are authoritative execution facts used to update orders, positions,
/// trades, fees, and execution accounting.
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `f64` quantity -> `Quantity`
/// - `f64` price -> `Price`
/// - `f64` fee -> `Money`
#[derive(Debug, Clone)]
pub struct Fill {
    /// Engine-owned unique execution identifier.
    pub execution_id: ExecutionId,

    /// Internal order that produced this fill.
    pub order_id: OrderId,

    /// Canonical execution venue where the fill occurred.
    ///
    /// Venue-specific API behavior remains outside the engine, but retaining
    /// canonical venue identity allows fills from multiple execution venues
    /// to coexist safely in the same engine state.
    pub venue: Venue,

    /// Execution identifier reported by the venue when available.
    ///
    /// Hyperliquid, for example, stores its `tid` here.
    ///
    /// This is separate from the engine-owned `ExecutionId`.
    pub venue_execution_id: Option<String>,

    /// Instrument that was executed.
    pub symbol: String,

    /// Buy or sell side of the execution.
    pub side: Side,

    /// Quantity executed by this individual fill.
    ///
    /// TODO: Replace with `Quantity`.
    pub quantity: f64,

    /// Execution price of this individual fill.
    ///
    /// TODO: Replace with `Price`.
    pub price: f64,

    /// Fee charged for this execution when provided by the venue.
    ///
    /// TODO: Replace with `Option<Money>`.
    pub fee: Option<f64>,

    /// Timestamp when the execution occurred.
    pub ts_ms: i64,
}




