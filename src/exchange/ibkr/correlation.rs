#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::execution::OrderId;


// ============================================================
// IBKR ORDER REFERENCE
// ============================================================

/// Adapter-local information required to reference an existing IBKR order.
///
/// This is transport/correlation state only.
///
/// It is NOT a duplicate canonical `Order` and must not contain authoritative
/// lifecycle, quantity, fill, price, position, trade, or risk state.
///
/// IBKR assigns an integer order ID when the order is submitted through TWS.
///
/// Correlation therefore maps:
///
///     Engine OrderId
///         ↕
///     IBKR order ID
#[derive(Debug, Clone, Copy)]
pub struct IbkrOrderRef {
    pub ibkr_order_id: i32,
}


// ============================================================
// IBKR ORDER CORRELATION
// ============================================================

/// Adapter-local correlation between canonical Engine OrderIds and IBKR
/// order IDs.
///
/// Lookup directions:
///
/// IBKR execution update:
///
///     IBKR order ID
///         ↓
///     Engine OrderId
///
/// Engine command:
///
///     Engine OrderId
///         ↓
///     IBKR order ID
///
/// This structure contains transport identity only.
///
/// Canonical Orders, Fills, Positions, Trades, and Risk remain exclusively
/// owned by the Engine.
#[derive(Debug, Default)]
pub struct IbkrOrderCorrelation {
    by_ibkr_order_id: HashMap<i32, OrderId>,
    by_order_id: HashMap<OrderId, IbkrOrderRef>,
}

impl IbkrOrderCorrelation {
    // ============================================================
    // NEW
    // ============================================================

    pub fn new() -> Self {
        Self::default()
    }


    // ============================================================
    // INSERT
    // ============================================================

    /// Registers the IBKR order ID returned after submitting a canonical
    /// Engine order through TWS.
    pub fn insert(&mut self, order_id: OrderId, ibkr_order_id: i32) {
        // Remove an older IBKR index if this Engine order was already
        // associated with another IBKR order ID.
        if let Some(previous) = self.by_order_id.insert(order_id, IbkrOrderRef { ibkr_order_id }) {
            self.by_ibkr_order_id.remove(&previous.ibkr_order_id);
        }

        self.by_ibkr_order_id.insert(ibkr_order_id, order_id);
    }


    // ============================================================
    // GET BY IBKR ORDER ID
    // ============================================================

    /// Resolves an IBKR order ID into the canonical Engine OrderId.
    pub fn get_by_ibkr_order_id(&self, ibkr_order_id: i32) -> Option<OrderId> {
        self.by_ibkr_order_id.get(&ibkr_order_id).copied()
    }


    // ============================================================
    // GET BY ORDER ID
    // ============================================================

    /// Resolves a canonical Engine OrderId into IBKR transport identity.
    pub fn get_by_order_id(&self, order_id: OrderId) -> Option<&IbkrOrderRef> {
        self.by_order_id.get(&order_id)
    }


    // ============================================================
    // REMOVE BY ORDER ID
    // ============================================================

    /// Removes all correlation indexes belonging to an order.
    ///
    /// Do not immediately remove correlation when the first terminal update
    /// arrives. IBKR may still deliver later execution/status messages.
    /// Cleanup should eventually use an adapter retention/reconciliation
    /// policy.
    pub fn remove_by_order_id(&mut self, order_id: OrderId) -> Option<IbkrOrderRef> {
        let order_ref = self.by_order_id.remove(&order_id)?;
        self.by_ibkr_order_id.remove(&order_ref.ibkr_order_id);
        Some(order_ref)
    }


    // ============================================================
    // LEN
    // ============================================================

    pub fn len(&self) -> usize {
        self.by_order_id.len()
    }


    // ============================================================
    // IS EMPTY
    // ============================================================

    pub fn is_empty(&self) -> bool {
        self.by_order_id.is_empty()
    }
}


// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ibkr_order_correlation() {
        let order_id = OrderId::new(42);
        let ibkr_order_id = 123;

        let mut correlation = IbkrOrderCorrelation::new();

        correlation.insert(order_id, ibkr_order_id);

        assert_eq!(correlation.get_by_ibkr_order_id(ibkr_order_id), Some(order_id));

        let order_ref = correlation
            .get_by_order_id(order_id)
            .expect("Expected IBKR order correlation");

        assert_eq!(order_ref.ibkr_order_id, ibkr_order_id);
        assert_eq!(correlation.len(), 1);

        let removed = correlation
            .remove_by_order_id(order_id)
            .expect("Expected IBKR correlation removal");

        assert_eq!(removed.ibkr_order_id, ibkr_order_id);
        assert!(correlation.get_by_ibkr_order_id(ibkr_order_id).is_none());
        assert!(correlation.get_by_order_id(order_id).is_none());
        assert!(correlation.is_empty());
    }
}