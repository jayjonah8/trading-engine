use crate::core::execution::{
    order::OrderId,
    side::Side,
};

/// Unique identifier for an individual execution/fill.
///
/// This ID is engine-owned and is used by the `FillLedger` to ensure the
/// same execution is never applied to engine state more than once.
///
/// TODO: Move this into a dedicated `fill/id.rs` if fill identity grows
/// beyond this initial execution model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecutionId(pub u64);

/// Immutable record of an actual order execution.
///
/// A single `Order` may produce multiple fills at different quantities and
/// prices. Each fill represents an execution that actually occurred and
/// should never be modified after it has been recorded.
///
/// Fills are the authoritative facts used to update orders, positions,
/// trades, fees, and execution accounting.
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `String` venue -> `Venue`
/// - `f64` quantity -> `Quantity`
/// - `f64` price -> `Price`
/// - `f64` fee -> `Money`
#[derive(Debug, Clone)]
pub struct Fill {
    /// Engine-owned unique execution identifier.
    pub execution_id: ExecutionId,

    /// Internal order that produced this fill.
    pub order_id: OrderId,

    /// Venue where the execution occurred.
    ///
    /// TODO: Replace with `Venue`.
    pub venue: String,

    /// Execution identifier reported by the venue when available.
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