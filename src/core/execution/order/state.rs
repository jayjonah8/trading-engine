/// Lifecycle state of an order inside the trading engine.
///
/// Partial fills are intentionally NOT represented as a separate state.
/// An order is partially filled when it remains `Working` while its
/// `filled_quantity` is greater than zero and less than its total quantity.
///
/// This avoids state explosion such as `PartiallyFilledPendingCancel`.
/// For example, an order may remain `PendingCancel` while receiving another
/// fill before the venue confirms the cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderState {
    /// Created by the engine but not yet acknowledged by the venue.
    PendingSubmit,

    /// Accepted by the venue and eligible to receive fills.
    Working,

    /// Cancellation has been requested but not yet confirmed.
    PendingCancel,

    /// Replacement/modification has been requested but not yet confirmed.
    PendingReplace,

    /// The entire requested quantity has been filled.
    Filled,

    /// The venue confirmed the remaining quantity was canceled.
    ///
    /// A canceled order may still have a non-zero filled quantity if it
    /// received partial fills before cancellation.
    Canceled,

    /// The venue rejected the order.
    Rejected,

    /// The order expired according to its venue or time-in-force rules.
    Expired,
}

impl OrderState {
    /// Returns true when no further normal lifecycle transition is expected.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired
        )
    }

    /// Returns true while the order is still active within the engine.
    pub const fn is_active(self) -> bool {
        !self.is_terminal()
    }
}