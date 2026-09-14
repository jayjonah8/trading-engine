#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::execution::{
    ClientOrderId,
    OrderId,
};


// ============================================================
// HYPERLIQUID ORDER CORRELATION
// ============================================================

/// Adapter-local correlation between the Engine's globally unique
/// `ClientOrderId` and its internal `OrderId`.
///
/// Hyperliquid receives `ClientOrderId` as CLOID. Execution WebSocket events
/// later return that CLOID, allowing the adapter to recover the canonical
/// `OrderId` required by `ExecEvent`.
///
/// This is NOT authoritative order state.
///
/// The Engine remains the sole owner of canonical Order state. This map exists
/// only to correlate asynchronous Hyperliquid messages with Engine orders.
///
/// Example:
///
/// Engine:
///
///     OrderId       = 42
///     ClientOrderId = f8ad4c63-8f84-4518-b414-62960f7332b2
///
/// Hyperliquid WS:
///
///     cloid = 0xf8ad4c638f844518b41462960f7332b2
///
/// Correlation:
///
///     CLOID
///       -> ClientOrderId
///       -> OrderId(42)
///       -> ExecEvent
#[derive(Debug, Default)]
pub struct HyperliquidOrderCorrelation {
    by_client_order_id: HashMap<ClientOrderId, OrderId>,
}


impl HyperliquidOrderCorrelation {
    // ============================================================
    // NEW
    // ============================================================

    pub fn new() -> Self {
        Self::default()
    }


    // ============================================================
    // INSERT
    // ============================================================

    /// Registers an Engine order before it is submitted to Hyperliquid.
    ///
    /// Registration should happen BEFORE awaiting venue submission so a fast
    /// WebSocket event cannot arrive before its correlation entry exists.
    pub fn insert(
        &mut self,
        client_order_id: ClientOrderId,
        order_id: OrderId,
    ) {
        self.by_client_order_id.insert(
            client_order_id,
            order_id,
        );
    }


    // ============================================================
    // GET
    // ============================================================

    /// Resolves a canonical ClientOrderId back to its Engine OrderId.
    pub fn get(
        &self,
        client_order_id: ClientOrderId,
    ) -> Option<OrderId> {
        self.by_client_order_id
            .get(&client_order_id)
            .copied()
    }


    // ============================================================
    // REMOVE
    // ============================================================

    /// Removes a correlation entry.
    ///
    /// Do not remove an entry merely because OrderUpdates reports a terminal
    /// status. UserEvents fills may arrive independently and still require the
    /// correlation.
    ///
    /// Cleanup policy will be added once terminal lifecycle reconciliation is
    /// implemented.
    pub fn remove(
        &mut self,
        client_order_id: ClientOrderId,
    ) -> Option<OrderId> {
        self.by_client_order_id
            .remove(&client_order_id)
    }


    // ============================================================
    // LEN
    // ============================================================

    pub fn len(
        &self,
    ) -> usize {
        self.by_client_order_id.len()
    }


    // ============================================================
    // IS EMPTY
    // ============================================================

    pub fn is_empty(
        &self,
    ) -> bool {
        self.by_client_order_id.is_empty()
    }
}


// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::execution::OrderId;


    #[test]
    fn test_hyperliquid_order_correlation() {
        let client_order_id =
            ClientOrderId::new();

        let order_id =
            OrderId::new(42);


        let mut correlation =
            HyperliquidOrderCorrelation::new();


        assert!(
            correlation
                .get(client_order_id)
                .is_none()
        );


        correlation.insert(
            client_order_id,
            order_id,
        );


        assert_eq!(
            correlation.get(client_order_id),
            Some(order_id)
        );


        assert_eq!(
            correlation.len(),
            1
        );
    }
}