#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::execution::{
    ClientOrderId,
    OrderId,
};


// ============================================================
// HYPERLIQUID ORDER REFERENCE
// ============================================================

/// Adapter-local information required to reference an existing
/// Hyperliquid order.
///
/// This is transport/correlation state only.
///
/// It is NOT a duplicate canonical `Order` and must not contain
/// authoritative lifecycle, quantity, fill, price, or risk state.
///
/// The Hyperliquid adapter needs:
///
/// - `client_order_id`
///     Engine-generated identity sent to Hyperliquid as CLOID.
///
/// - `symbol`
///     Required by Hyperliquid when canceling an order.
///
/// Example cancellation path:
///
///     OrderId
///         ↓
///     HyperliquidOrderRef
///         ↓
///     ClientOrderId -> CLOID
///     symbol        -> asset
///         ↓
///     Hyperliquid cancel_by_cloid(...)
#[derive(Debug, Clone)]
pub struct HyperliquidOrderRef {
    pub client_order_id: ClientOrderId,
    pub symbol: String,
}


// ============================================================
// HYPERLIQUID ORDER CORRELATION
// ============================================================

/// Adapter-local correlation between canonical Engine order identities and
/// the identifiers required by Hyperliquid.
///
/// Two indexes are maintained because execution events and Engine commands
/// arrive from opposite directions.
///
/// WebSocket execution-event path:
///
///     Hyperliquid CLOID
///         ↓
///     ClientOrderId
///         ↓
///     by_client_order_id
///         ↓
///     OrderId
///
/// Engine cancellation path:
///
///     OrderId
///         ↓
///     by_order_id
///         ↓
///     HyperliquidOrderRef
///         ├── ClientOrderId -> CLOID
///         └── symbol        -> asset
///
/// This structure is NOT authoritative order state.
///
/// Canonical Orders, lifecycle state, fills, quantities, positions,
/// trades, and risk state remain exclusively owned by the Engine.
#[derive(Debug, Default)]
pub struct HyperliquidOrderCorrelation {
    /// Used primarily for asynchronous Hyperliquid WebSocket events.
    by_client_order_id:
        HashMap<ClientOrderId, OrderId>,

    /// Used primarily for Engine commands that reference an existing
    /// order only through its canonical OrderId.
    by_order_id:
        HashMap<OrderId, HyperliquidOrderRef>,
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

    /// Registers a canonical Engine order with the Hyperliquid adapter.
    ///
    /// Registration must happen BEFORE the venue submission task is spawned.
    ///
    /// Hyperliquid may emit execution WebSocket events immediately after
    /// receiving an order, so correlation must already exist when those
    /// messages arrive.
    pub fn insert(
        &mut self,
        order_id: OrderId,
        client_order_id: ClientOrderId,
        symbol: String,
    ) {
        // --------------------------------------------------------
        // WS LOOKUP
        // --------------------------------------------------------

        self.by_client_order_id.insert(
            client_order_id,
            order_id,
        );


        // --------------------------------------------------------
        // ENGINE COMMAND LOOKUP
        // --------------------------------------------------------

        self.by_order_id.insert(
            order_id,
            HyperliquidOrderRef {
                client_order_id,
                symbol,
            },
        );
    }


    // ============================================================
    // GET BY CLIENT ORDER ID
    // ============================================================

    /// Resolves a Hyperliquid CLOID-derived `ClientOrderId` into the
    /// Engine's canonical `OrderId`.
    ///
    /// Used primarily by:
    ///
    /// - OrderUpdates
    /// - UserEvents::Fills
    pub fn get_by_client_order_id(
        &self,
        client_order_id: ClientOrderId,
    ) -> Option<OrderId> {
        self.by_client_order_id
            .get(&client_order_id)
            .copied()
    }


    // ============================================================
    // GET BY ORDER ID
    // ============================================================

    /// Resolves an Engine `OrderId` into the adapter-local information
    /// required to address the corresponding Hyperliquid order.
    ///
    /// Used primarily by cancellation and later replacement commands.
    pub fn get_by_order_id(
        &self,
        order_id: OrderId,
    ) -> Option<&HyperliquidOrderRef> {
        self.by_order_id
            .get(&order_id)
    }


    // ============================================================
    // REMOVE BY ORDER ID
    // ============================================================

    /// Removes both correlation indexes for an order.
    ///
    /// IMPORTANT:
    ///
    /// Do not remove correlation merely because `OrderUpdates` reports a
    /// terminal lifecycle status.
    ///
    /// Hyperliquid `UserEvents::Fills` may arrive independently and still
    /// require CLOID -> OrderId correlation.
    ///
    /// A production cleanup/reconciliation policy should determine when an
    /// order is safe to remove.
    pub fn remove_by_order_id(
        &mut self,
        order_id: OrderId,
    ) -> Option<HyperliquidOrderRef> {
        let order_ref =
            self.by_order_id.remove(
                &order_id
            )?;


        self.by_client_order_id.remove(
            &order_ref.client_order_id
        );


        Some(order_ref)
    }


    // ============================================================
    // LEN
    // ============================================================

    /// Number of canonical orders currently tracked by correlation.
    ///
    /// Both internal maps contain indexes for the same logical orders,
    /// therefore only one map is counted.
    pub fn len(
        &self,
    ) -> usize {
        self.by_order_id.len()
    }


    // ============================================================
    // IS EMPTY
    // ============================================================

    pub fn is_empty(
        &self,
    ) -> bool {
        self.by_order_id.is_empty()
    }
}


// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;


    // ============================================================
    // BIDIRECTIONAL CORRELATION
    // ============================================================

    #[test]
    fn test_hyperliquid_order_correlation() {
        let order_id =
            OrderId::new(42);

        let client_order_id =
            ClientOrderId::new();

        let symbol =
            "BTC".to_string();


        let mut correlation =
            HyperliquidOrderCorrelation::new();


        // --------------------------------------------------------
        // EMPTY
        // --------------------------------------------------------

        assert!(
            correlation
                .get_by_client_order_id(
                    client_order_id
                )
                .is_none()
        );

        assert!(
            correlation
                .get_by_order_id(
                    order_id
                )
                .is_none()
        );


        // --------------------------------------------------------
        // INSERT
        // --------------------------------------------------------

        correlation.insert(
            order_id,
            client_order_id,
            symbol.clone(),
        );


        // --------------------------------------------------------
        // CLIENT ORDER ID -> ORDER ID
        // --------------------------------------------------------

        assert_eq!(
            correlation
                .get_by_client_order_id(
                    client_order_id
                ),
            Some(order_id)
        );


        // --------------------------------------------------------
        // ORDER ID -> HYPERLIQUID ORDER REF
        // --------------------------------------------------------

        let order_ref =
            correlation
                .get_by_order_id(
                    order_id
                )
                .expect(
                    "Expected Hyperliquid order correlation"
                );


        assert_eq!(
            order_ref.client_order_id,
            client_order_id
        );

        assert_eq!(
            order_ref.symbol,
            symbol
        );


        assert_eq!(
            correlation.len(),
            1
        );


        // --------------------------------------------------------
        // REMOVE
        // --------------------------------------------------------

        let removed =
            correlation
                .remove_by_order_id(
                    order_id
                )
                .expect(
                    "Expected correlation removal"
                );


        assert_eq!(
            removed.client_order_id,
            client_order_id
        );

        assert_eq!(
            removed.symbol,
            "BTC"
        );


        // Both indexes must disappear together.
        assert!(
            correlation
                .get_by_client_order_id(
                    client_order_id
                )
                .is_none()
        );

        assert!(
            correlation
                .get_by_order_id(
                    order_id
                )
                .is_none()
        );


        assert!(
            correlation.is_empty()
        );
    }
}