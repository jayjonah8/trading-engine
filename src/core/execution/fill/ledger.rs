#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::execution::fill::fill::{ExecutionId, Fill};

/// Authoritative ledger of executions processed by the engine.
///
/// Every accepted fill is stored here permanently for the lifetime of the
/// engine. The ledger also provides duplicate execution protection so that
/// reconnects, polling, reconciliation, or repeated venue events cannot
/// accidentally apply the same execution more than once.
///
/// A fill should be inserted into the ledger BEFORE it is applied to order,
/// position, trade, or accounting state.
///
/// TODO: Add persistent venue-level fill deduplication using a stable key such
/// as `(Venue, venue_execution_id)`. The current engine-owned `ExecutionId`
/// protects against duplicates during a single runtime, but is not sufficient
/// across engine restarts or reconciliation where internal IDs may be regenerated.
#[derive(Debug, Default)]
pub struct FillLedger {
    fills: HashMap<ExecutionId, Fill>,
}

impl FillLedger {
    /// Creates an empty fill ledger.
    pub fn new() -> Self {
        Self {
            fills: HashMap::new(),
        }
    }

    /// Inserts a fill if its execution ID has not already been processed.
    ///
    /// Returns `true` when the fill was newly inserted.
    /// Returns `false` when the execution was already present and should
    /// therefore NOT be applied to engine state again.
    pub fn insert(&mut self, fill: Fill) -> bool {
        if self.fills.contains_key(&fill.execution_id) {
            return false;
        }

        self.fills.insert(fill.execution_id, fill);
        true
    }

    /// Returns a fill by its engine execution ID.
    pub fn get(&self, execution_id: ExecutionId) -> Option<&Fill> {
        self.fills.get(&execution_id)
    }

    /// Returns true if an execution has already been processed.
    pub fn contains(&self, execution_id: ExecutionId) -> bool {
        self.fills.contains_key(&execution_id)
    }

    /// Returns the total number of unique fills recorded.
    pub fn len(&self) -> usize {
        self.fills.len()
    }

    /// Returns true when no fills have been recorded.
    pub fn is_empty(&self) -> bool {
        self.fills.is_empty()
    }
}