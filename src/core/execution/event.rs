use crate::core::execution::{
    fill::Fill,
    order::OrderId,
};

/// Execution events reported by an execution venue back to the engine.
///
/// `ExecEvent` is completely venue-independent. Exchange adapters translate
/// venue-specific WebSocket, REST, polling, and reconciliation responses into
/// these canonical events.
///
/// These events describe execution facts only. They intentionally do not
/// contain strategy concepts such as "open trade", "close trade", or
/// "partial open fill". The engine determines an order's purpose by looking
/// up the associated `Order`.
#[derive(Debug, Clone)]
pub enum ExecEvent {
    /// The venue accepted the submitted order and assigned its own order ID.
    OrderAccepted {
        order_id: OrderId,
        venue_order_id: String,
        ts_ms: i64,
    },

    /// The order is active at the venue and capable of receiving fills.
    OrderWorking {
        order_id: OrderId,
        ts_ms: i64,
    },

    /// An execution occurred against the order.
    ///
    /// Every individual execution is represented as a `Fill`. Partial versus
    /// complete fill state is determined by the engine from the order's total
    /// `filled_quantity` after applying this fill.
    Fill {
        fill: Fill,
    },

    /// The venue confirmed cancellation of the remaining order quantity.
    ///
    /// The order may still have a non-zero filled quantity from executions
    /// that occurred before cancellation was confirmed.
    OrderCanceled {
        order_id: OrderId,
        ts_ms: i64,
    },

    /// The order expired according to venue or time-in-force rules.
    OrderExpired {
        order_id: OrderId,
        ts_ms: i64,
    },

    /// The venue rejected the order.
    OrderRejected {
        order_id: OrderId,
        reason: String,
        ts_ms: i64,
    },

    /// The venue confirmed that an existing order was replaced or modified.
    ///
    /// Some venues preserve the original venue order ID while others assign
    /// a new one. `venue_order_id` therefore contains the currently active
    /// venue-side identifier after replacement.
    OrderReplaced {
        order_id: OrderId,
        venue_order_id: String,
        ts_ms: i64,
    },

    /// Execution-layer error associated with an order.
    ///
    /// This represents an operational/API error and is distinct from an
    /// explicit venue rejection of the order.
    Error {
        order_id: Option<OrderId>,
        message: String,
        ts_ms: i64,
    },
}