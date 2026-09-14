#![allow(dead_code)]

use std::sync::atomic::{
    AtomicU64,
    Ordering,
};


// ============================================================
// EXECUTION ID
// ============================================================

/// Engine-owned unique identifier for one canonical execution/fill.
///
/// `ExecutionId` is intentionally separate from a venue's own execution ID.
///
/// Example:
///
/// Hyperliquid:
///
///     ExecutionId
///         = engine-owned runtime identity
///
///     venue_execution_id
///         = Hyperliquid `tid`
///
/// Keeping these identities separate prevents venue-specific identifiers from
/// becoming part of the canonical engine identity model.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct ExecutionId(pub u64);


// ============================================================
// EXECUTION ID ALLOCATOR
// ============================================================

/// Process-wide canonical execution ID allocator.
///
/// All execution adapters share this allocator, so canonical ExecutionIds
/// remain unique across Hyperliquid, Alpaca, IBKR, simulation, and other
/// execution sources during the current engine process.
///
/// Stable venue-level duplicate detection remains separate and uses:
///
///     (Venue, venue_execution_id)
///
/// inside FillLedger.
static NEXT_EXECUTION_ID: AtomicU64 =
    AtomicU64::new(1);


/// Allocates the next canonical engine execution ID.
pub fn allocate_execution_id() -> ExecutionId {
    ExecutionId(
        NEXT_EXECUTION_ID.fetch_add(
            1,
            Ordering::Relaxed,
        )
    )
}