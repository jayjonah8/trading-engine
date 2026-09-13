#![allow(dead_code)]

use crate::core::execution::order::OrderChanges;

/// Engine-owned record of an asynchronous order replacement currently waiting
/// for venue confirmation.
///
/// Order replacement is not instantaneous:
///
/// Working
///     ↓
/// replacement requested
///     ↓
/// validate requested changes
///     ↓
/// risk-check any increased exposure
///     ↓
/// store PendingOrderReplace
///     ↓
/// OrderState::PendingReplace
///     ↓
/// ExecCommand::ReplaceOrder
///     ↓
/// venue
///     ↓
/// ExecEvent::OrderReplaced
///     ↓
/// apply confirmed changes
///     ↓
/// adjust canonical reservation/state
///     ↓
/// clear pending replacement
///     ↓
/// OrderState::Working
///
/// The engine stores the requested changes here because the venue confirmation
/// event does not necessarily echo every modified field.
///
/// This also keeps the engine authoritative: it knows exactly what replacement
/// it requested rather than reconstructing state from venue-specific responses.
#[derive(Debug, Clone)]
pub struct PendingOrderReplace {
    /// Canonical changes requested by the engine.
    pub changes: OrderChanges,

    /// Engine/event timestamp when the replacement request was created.
    pub requested_ts_ms: i64,
}

impl PendingOrderReplace {
    /// Creates a pending replacement record.
    pub fn new(
        changes: OrderChanges,
        requested_ts_ms: i64,
    ) -> Self {
        Self {
            changes,
            requested_ts_ms,
        }
    }
}