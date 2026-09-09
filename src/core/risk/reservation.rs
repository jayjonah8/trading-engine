use crate::core::{
    execution::order::OrderId,
    trades::TradeId,
};

/// Describes why risk capacity has been reserved.
///
/// A reservation exists while an order has committed engine capacity that
/// must be accounted for before execution is fully complete.
///
/// Entry reservations prevent the engine from over-allocating new exposure.
///
/// Exit reservations prevent multiple working exit orders from reserving or
/// closing the same trade quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReservationPurpose {
    Entry,
    Exit,
}

/// Risk capacity reserved for a specific execution order.
///
/// Reservations bridge the gap between an order being created and its
/// resulting exposure being reflected in actual positions.
///
/// For example:
///
/// - A working entry order reserves exposure before it fills.
/// - A working exit order reserves existing trade quantity before it fills.
///
/// Reservations should be reduced as fills occur and released when the
/// associated order is canceled, rejected, expired, or otherwise completed.
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `String` strategy ID -> `StrategyId`
/// - `String` symbol -> `Symbol`
/// - `f64` quantity -> `Quantity`
/// - `f64` notional -> `Money`
#[derive(Debug, Clone)]
pub struct RiskReservation {
    /// Order responsible for this reservation.
    pub order_id: OrderId,

    /// Logical trade associated with this reservation, when applicable.
    ///
    /// Entry reservations may reference a trade that is still `Opening`.
    /// Exit reservations normally reference an existing trade.
    pub trade_id: Option<TradeId>,

    /// Strategy that owns the reservation.
    ///
    /// TODO: Replace with `StrategyId`.
    pub strategy_id: String,

    /// Instrument whose capacity is being reserved.
    ///
    /// TODO: Replace with canonical `Symbol`.
    pub symbol: String,

    /// Why this capacity is reserved.
    pub purpose: ReservationPurpose,

    /// Quantity currently reserved by the order.
    ///
    /// This value decreases as fills occur.
    ///
    /// TODO: Replace with `Quantity`.
    pub quantity: f64,

    /// Approximate notional value currently reserved.
    ///
    /// This is primarily useful for entry-side exposure and capital limits.
    ///
    /// TODO: Replace with `Money`.
    pub notional: f64,

    /// Timestamp when the reservation was created.
    pub created_ts_ms: i64,

    /// Timestamp when the reservation was last modified.
    pub updated_ts_ms: i64,
}