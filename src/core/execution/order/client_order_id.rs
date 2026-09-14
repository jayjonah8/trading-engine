#![allow(dead_code)]

use std::fmt;

use uuid::Uuid;


// ============================================================
// CLIENT ORDER ID
// ============================================================

/// Globally unique client-generated identity for a canonical Order.
///
/// `ClientOrderId` is separate from every other order/execution identity:
///
/// OrderId
///     = internal engine-owned numeric identifier
///     = optimized for fast in-process lookup
///
/// ClientOrderId
///     = globally unique client-generated identifier
///     = created by the Engine
///     = may be sent to execution venues when supported
///
/// VenueOrderId
///     = identifier assigned by the execution venue
///     = currently stored on Order as `venue_order_id`
///
/// VenueExecutionId
///     = identifier assigned to one individual execution/fill
///     = currently stored on Fill as `venue_execution_id`
///
/// Example:
//
/// Engine:
///
///     OrderId       = 42
///     ClientOrderId = 7b805495-2c28-47fa-bd31-01acd2048e21
///
/// Hyperliquid:
///
///     ClientOrderId -> CLOID
///     venue_order_id -> oid
///     venue_execution_id -> tid
///
/// Alpaca:
///
///     ClientOrderId -> client_order_id
///     venue_order_id -> Alpaca order ID
///
/// The Engine owns this identity. Venue adapters may translate it into their
/// venue-specific client-order identifier format, but they should not invent
/// a different canonical identity.
///
/// UUID v4 is used so identifiers remain unique across:
///
/// - engine restarts
/// - multiple strategies
/// - multiple symbols
/// - multiple processes
/// - multiple servers
/// - multiple execution venues
///
/// This avoids depending on the engine's local `OrderId`, which currently
/// restarts from 1 whenever a new EngineState is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientOrderId(Uuid);

impl ClientOrderId {
    /// Generates a new globally unique client-order identity.
    pub fn new() -> Self {
        Self(
            Uuid::new_v4()
        )
    }

    /// Creates a ClientOrderId from an existing UUID.
    ///
    /// Useful when restoring persisted orders or decoding venue events.
    pub const fn from_uuid(
        uuid: Uuid,
    ) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    pub const fn as_uuid(
        self,
    ) -> Uuid {
        self.0
    }

    /// Returns the raw 128-bit representation.
    ///
    /// This is particularly useful for venues whose client-order identifier
    /// is naturally represented as a 128-bit value.
    pub const fn as_u128(
        self,
    ) -> u128 {
        self.0.as_u128()
    }
}

impl Default for ClientOrderId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "{}",
            self.0
        )
    }
}