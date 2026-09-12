#![allow(dead_code)]


use crate::core::execution::order::{Order, OrderId};

// It’s simply SubmitOrder, CancelOrder, and ReplaceOrder. The Order itself tells 
// the adapter whether it’s market/limit, buy/sell, GTC/IOC, reduce-only, etc.


/// Commands sent from the trading engine to an execution venue.
///
/// `ExecCommand` is completely venue-independent. Exchange adapters translate
/// these commands into their venue-specific API operations.
///
/// The engine owns authoritative order state. Adapters execute commands and
/// report the resulting lifecycle changes back through `ExecEvent`.
#[derive(Debug, Clone)]
pub enum ExecCommand {
    /// Submit a new order to the execution venue.
    SubmitOrder {
        order: Order,
    },

    /// Request cancellation of an existing order.
    ///
    /// Cancellation is asynchronous. Sending this command does not mean the
    /// order is canceled yet. The engine should keep the order in
    /// `PendingCancel` until the venue confirms cancellation through an
    /// `ExecEvent`.
    CancelOrder {
        order_id: OrderId,
    },

    /// Request replacement/modification of an existing working order.
    ///
    /// Many venues implement modification internally as cancel-and-replace.
    /// The adapter is responsible for translating this into the correct
    /// venue-specific operation.
    ReplaceOrder {
        order_id: OrderId,
        changes: OrderChanges,
    },
}

/// Requested changes to an existing order.
///
/// Only fields containing `Some(...)` should be changed. All other order
/// properties remain unchanged.
///
/// TODO: Replace `f64` with canonical `Quantity` and `Price` types.
#[derive(Debug, Clone, Default)]
pub struct OrderChanges {
    /// New total requested quantity.
    pub quantity: Option<f64>,

    /// New limit price.
    pub limit_px: Option<f64>,

    /// New stop trigger price.
    pub stop_px: Option<f64>,
}