#![allow(dead_code)]

use tokio::sync::oneshot;

use crate::core::execution::{
    order::{
        Order,
        OrderChanges,
        OrderId,
    },
    Venue,
};

// The canonical execution command surface is intentionally small:
//
// - SubmitOrder
// - CancelOrder
// - ReplaceOrder
// - BacktestBarrier (internal synchronization)
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
#[derive(Debug)]
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

    // ========================================================
    // BACKTEST SYNCHRONIZATION
    // ========================================================

    /// Synchronizes historical execution-command processing.
    ///
    /// This is an internal backtest-only command.
    /// The backtest adapter acknowledges it after processing all
    /// previously queued execution commands.
    ///
    /// `venue` identifies which registered backtest adapter should
    /// receive the synchronization command.
    ///
    /// This supports historical backtesting across all venues.
    BacktestBarrier {
        venue: Venue,
        done: oneshot::Sender<()>,
    },
}

impl ExecCommand {
    /// Returns the execution venue responsible for this command.
    ///
    /// This gives the execution router a single venue-independent way to
    /// route SubmitOrder, CancelOrder, ReplaceOrder, and BacktestBarrier.
    ///
    /// For new orders the venue comes from the canonical `Order`.
    /// Cancel, replace, and backtest synchronization commands carry
    /// the venue directly.
    ///
    /// Returns `Some(Venue)` for venue-routed commands.
    /// `None` is reserved for future internal commands that do not
    /// require a specific execution venue.
    pub const fn venue(&self) -> Option<Venue> {
        match self {
            Self::SubmitOrder { order } => {
                Some(order.venue)
            }

            Self::CancelOrder { venue, .. } => {
                Some(*venue)
            }

            Self::ReplaceOrder { venue, .. } => {
                Some(*venue)
            }

            Self::BacktestBarrier { venue, .. } => {
                Some(*venue)
            }
        }
    }
}