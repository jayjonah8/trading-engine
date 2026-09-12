#![allow(dead_code)]

use crate::core::execution::{
    order::{
        Order,
        OrderId,
    },
    Venue,
};

// The canonical execution command surface is intentionally small:
//
// - SubmitOrder
// - CancelOrder
// - ReplaceOrder
//
// Order-specific execution details such as market/limit, buy/sell,
// time-in-force, reduce-only, and post-only live on `Order`.
//
// Venue-specific API behavior remains inside the execution adapters.


/// Commands sent from the trading engine to an execution venue.
///
/// `ExecCommand` is completely venue-independent. Exchange adapters translate
/// these commands into their venue-specific API operations.
///
/// Every command contains enough routing information for the execution router
/// to determine which venue adapter should receive it.
///
/// The engine owns authoritative order state. Adapters execute commands and
/// report resulting lifecycle changes back through canonical `ExecEvent`s.
#[derive(Debug, Clone)]
pub enum ExecCommand {
    /// Submit a new order to its configured execution venue.
    ///
    /// The destination venue is stored directly on `Order`.
    SubmitOrder {
        order: Order,
    },

    /// Request cancellation of an existing order.
    ///
    /// `venue` allows the execution router to forward the request to the
    /// adapter that owns the order without requiring the router to maintain
    /// its own copy of engine order state.
    ///
    /// Cancellation is asynchronous. Sending this command does not mean the
    /// order is canceled yet. The engine should keep the order in
    /// `PendingCancel` until the venue confirms cancellation through an
    /// `ExecEvent`.
    CancelOrder {
        venue: Venue,
        order_id: OrderId,
    },

    /// Request replacement/modification of an existing working order.
    ///
    /// `venue` identifies the adapter responsible for the existing order.
    ///
    /// Some venues support native modification while others may implement
    /// this as cancel-and-replace. The adapter is responsible for translating
    /// this canonical request into the correct venue-specific operation.
    ReplaceOrder {
        venue: Venue,
        order_id: OrderId,
        changes: OrderChanges,
    },
}

impl ExecCommand {
    /// Returns the execution venue responsible for this command.
    ///
    /// This gives the execution router a single venue-independent way to
    /// route SubmitOrder, CancelOrder, and ReplaceOrder commands.
    ///
    /// For new orders the venue comes from the canonical `Order`.
    /// Cancel and replace commands carry the venue directly because they
    /// contain only the internal `OrderId`, not the complete `Order`.
    pub const fn venue(&self) -> Venue {
        match self {
            Self::SubmitOrder { order } => {
                order.venue
            }

            Self::CancelOrder { venue, .. } => {
                *venue
            }

            Self::ReplaceOrder { venue, .. } => {
                *venue
            }
        }
    }
}


// ============================================================
// ORDER CHANGES
// ============================================================

/// Requested changes to an existing order.
///
/// Only fields containing `Some(...)` should be changed. All other order
/// properties remain unchanged.
///
/// The execution adapter determines how the requested changes are implemented
/// by its venue. For example, a venue may support native modification while
/// another may require cancel-and-replace.
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