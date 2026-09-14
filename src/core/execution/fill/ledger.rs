#![allow(dead_code)]

use std::collections::{
    HashMap,
    HashSet,
};

use crate::core::execution::{
    fill::{
        ExecutionId,
        Fill,
    },
    Venue,
};


// ============================================================
// VENUE EXECUTION KEY
// ============================================================

/// Stable venue-level identity for an execution.
///
/// This protects the Engine from applying the same real venue execution more
/// than once when it is observed through multiple execution paths such as:
///
/// - WebSocket
/// - reconnect/replay
/// - polling
/// - reconciliation
///
/// Examples:
///
/// Hyperliquid:
///
///     venue              = Venue::Hyperliquid
///     venue_execution_id = tid
///
/// Other venues may use their own stable execution/fill identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VenueExecutionKey {
    venue: Venue,
    venue_execution_id: String,
}


// ============================================================
// FILL LEDGER
// ============================================================

/// Authoritative ledger of executions processed by the Engine.
///
/// Every accepted Fill is stored here before it is applied to Order,
/// Position, Trade, or accounting state.
///
/// Duplicate protection exists at two levels:
///
/// 1. `ExecutionId`
///      canonical Engine execution identity
///
/// 2. `(Venue, venue_execution_id)`
///      stable venue execution identity
///
/// The second check is especially important for reconnects and
/// reconciliation because the same venue execution may be observed again.
#[derive(Debug, Default)]
pub struct FillLedger {
    fills: HashMap<ExecutionId, Fill>,

    venue_executions: HashSet<VenueExecutionKey>,
}


// ============================================================
// IMPLEMENTATION
// ============================================================

impl FillLedger {
    /// Creates an empty FillLedger.
    pub fn new() -> Self {
        Self {
            fills: HashMap::new(),
            venue_executions: HashSet::new(),
        }
    }


    // ========================================================
    // INSERT
    // ========================================================

    /// Inserts a Fill only if it has not already been processed.
    ///
    /// Returns:
    ///
    /// `true`
    ///     Fill is new and should be applied to Engine state.
    ///
    /// `false`
    ///     Fill is a duplicate and must NOT be applied again.
    pub fn insert(
        &mut self,
        fill: Fill,
    ) -> bool {
        // --------------------------------------------------------
        // CANONICAL EXECUTION ID
        // --------------------------------------------------------

        if self.fills.contains_key(
            &fill.execution_id
        ) {
            return false;
        }


        // --------------------------------------------------------
        // VENUE EXECUTION ID
        // --------------------------------------------------------

        let venue_key =
            fill.venue_execution_id
                .as_ref()
                .map(|venue_execution_id| {
                    VenueExecutionKey {
                        venue: fill.venue,
                        venue_execution_id:
                            venue_execution_id.clone(),
                    }
                });


        if let Some(key) =
            venue_key.as_ref()
        {
            if self.venue_executions.contains(
                key
            ) {
                return false;
            }
        }


        // --------------------------------------------------------
        // COMMIT
        // --------------------------------------------------------

        if let Some(key) =
            venue_key
        {
            self.venue_executions.insert(
                key
            );
        }


        self.fills.insert(
            fill.execution_id,
            fill,
        );


        true
    }


    // ========================================================
    // GET
    // ========================================================

    /// Returns a Fill by its canonical Engine execution ID.
    pub fn get(
        &self,
        execution_id: ExecutionId,
    ) -> Option<&Fill> {
        self.fills.get(
            &execution_id
        )
    }


    // ========================================================
    // CONTAINS
    // ========================================================

    /// Returns true if a canonical execution ID has already been processed.
    pub fn contains(
        &self,
        execution_id: ExecutionId,
    ) -> bool {
        self.fills.contains_key(
            &execution_id
        )
    }


    // ========================================================
    // VENUE EXECUTION CONTAINS
    // ========================================================

    /// Returns true if a venue execution has already been processed.
    pub fn contains_venue_execution(
        &self,
        venue: Venue,
        venue_execution_id: &str,
    ) -> bool {
        self.venue_executions.contains(
            &VenueExecutionKey {
                venue,
                venue_execution_id:
                    venue_execution_id.to_string(),
            }
        )
    }


    // ========================================================
    // LEN
    // ========================================================

    /// Returns the total number of unique canonical Fills recorded.
    pub fn len(&self) -> usize {
        self.fills.len()
    }


    // ========================================================
    // EMPTY
    // ========================================================

    /// Returns true when no Fills have been recorded.
    pub fn is_empty(&self) -> bool {
        self.fills.is_empty()
    }
}

// old
// #![allow(dead_code)]

// use std::collections::HashMap;

// use crate::core::execution::fill::{
//     ExecutionId,
//     Fill,
// };

// /// Authoritative ledger of executions processed by the engine.
// ///
// /// Every accepted fill is stored here permanently for the lifetime of the
// /// engine. The ledger also provides duplicate execution protection so that
// /// reconnects, polling, reconciliation, or repeated venue events cannot
// /// accidentally apply the same execution more than once.
// ///
// /// A fill should be inserted into the ledger BEFORE it is applied to order,
// /// position, trade, or accounting state.
// ///
// /// TODO: Add persistent venue-level fill deduplication using a stable key such
// /// as `(Venue, venue_execution_id)`. The current engine-owned `ExecutionId`
// /// protects against duplicates during a single runtime, but is not sufficient
// /// across engine restarts or reconciliation where internal IDs may be regenerated.
// #[derive(Debug, Default)]
// pub struct FillLedger {
//     fills: HashMap<ExecutionId, Fill>,
// }

// impl FillLedger {
//     /// Creates an empty fill ledger.
//     pub fn new() -> Self {
//         Self {
//             fills: HashMap::new(),
//         }
//     }

//     /// Inserts a fill if its execution ID has not already been processed.
//     ///
//     /// Returns `true` when the fill was newly inserted.
//     /// Returns `false` when the execution was already present and should
//     /// therefore NOT be applied to engine state again.
//     pub fn insert(&mut self, fill: Fill) -> bool {
//         if self.fills.contains_key(&fill.execution_id) {
//             return false;
//         }

//         self.fills.insert(fill.execution_id, fill);
//         true
//     }

//     /// Returns a fill by its engine execution ID.
//     pub fn get(&self, execution_id: ExecutionId) -> Option<&Fill> {
//         self.fills.get(&execution_id)
//     }

//     /// Returns true if an execution has already been processed.
//     pub fn contains(&self, execution_id: ExecutionId) -> bool {
//         self.fills.contains_key(&execution_id)
//     }

//     /// Returns the total number of unique fills recorded.
//     pub fn len(&self) -> usize {
//         self.fills.len()
//     }

//     /// Returns true when no fills have been recorded.
//     pub fn is_empty(&self) -> bool {
//         self.fills.is_empty()
//     }
// }