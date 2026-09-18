#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::execution::{
    ClientOrderId,
    OrderId,
};


// ============================================================
// ALPACA ORDER REFERENCE
// ============================================================

/// Adapter-local information required to reference an existing Alpaca order.
///
/// This is transport/correlation state only.
///
/// It is NOT a duplicate canonical `Order` and must not contain authoritative
/// lifecycle, quantity, fill, price, position, trade, or risk state.
///
/// Alpaca supports the Engine-generated `ClientOrderId` directly through its
/// `client_order_id` field.
///
/// Alpaca also assigns its own venue order ID after accepting the order.
///
/// Therefore the adapter may eventually know:
///
///     OrderId
///         ↓
///     AlpacaOrderRef
///         ├── ClientOrderId
///         └── venue_order_id
///
/// `venue_order_id` is optional initially because correlation is registered
/// before the asynchronous REST submission begins.
#[derive(Debug, Clone)]
pub struct AlpacaOrderRef {
    pub client_order_id: ClientOrderId,
    pub venue_order_id: Option<String>,
}


// ============================================================
// ALPACA ORDER CORRELATION
// ============================================================

/// Adapter-local correlation between canonical Engine order identities and
/// Alpaca order identities.
///
/// Three lookup directions are supported:
///
/// Engine-generated client order ID:
///
///     Alpaca client_order_id
///         ↓
///     ClientOrderId
///         ↓
///     OrderId
///
/// Alpaca-generated venue order ID:
///
///     Alpaca order ID
///         ↓
///     OrderId
///
/// Engine command:
///
///     OrderId
///         ↓
///     AlpacaOrderRef
///
/// Correlation is registered BEFORE REST submission so a fast Alpaca
/// `trade_updates` event can already resolve the canonical OrderId.
///
/// The Alpaca venue order ID is added later once REST submission or an
/// execution update provides it.
///
/// This structure is adapter-local transport state only. Canonical Orders,
/// Trades, Positions, Fills, and risk remain owned by the Engine.
#[derive(Debug, Default)]
pub struct AlpacaOrderCorrelation {
    by_client_order_id: HashMap<ClientOrderId, OrderId>,
    by_venue_order_id: HashMap<String, OrderId>,
    by_order_id: HashMap<OrderId, AlpacaOrderRef>,
}


impl AlpacaOrderCorrelation {

    // ============================================================
    // NEW
    // ============================================================

    pub fn new() -> Self {
        Self::default()
    }


    // ============================================================
    // INSERT
    // ============================================================

    /// Registers an Engine order before Alpaca REST submission.
    ///
    /// The venue order ID is not known yet.
    pub fn insert(
        &mut self,
        order_id: OrderId,
        client_order_id: ClientOrderId,
    ) {
        self.by_client_order_id.insert(
            client_order_id,
            order_id,
        );

        self.by_order_id.insert(
            order_id,
            AlpacaOrderRef {
                client_order_id,
                venue_order_id: None,
            },
        );
    }


    // ============================================================
    // SET VENUE ORDER ID
    // ============================================================

    /// Adds Alpaca's venue-assigned order ID after it becomes known.
    ///
    /// This may happen from the REST submission response or from an
    /// asynchronous `trade_updates` message.
    pub fn set_venue_order_id(
        &mut self,
        order_id: OrderId,
        venue_order_id: String,
    ) -> bool {
        let Some(order_ref) =
            self.by_order_id.get_mut(
                &order_id
            )
        else {
            return false;
        };


        // Remove an older venue-order index if this order was previously
        // associated with another Alpaca order ID.
        if let Some(previous_venue_order_id) =
            order_ref.venue_order_id.replace(
                venue_order_id.clone()
            )
        {
            self.by_venue_order_id.remove(
                &previous_venue_order_id
            );
        }


        self.by_venue_order_id.insert(
            venue_order_id,
            order_id,
        );


        true
    }


    // ============================================================
    // GET BY CLIENT ORDER ID
    // ============================================================

    /// Resolves Alpaca's `client_order_id` into the canonical Engine OrderId.
    pub fn get_by_client_order_id(
        &self,
        client_order_id: ClientOrderId,
    ) -> Option<OrderId> {
        self.by_client_order_id
            .get(&client_order_id)
            .copied()
    }


    // ============================================================
    // GET BY VENUE ORDER ID
    // ============================================================

    /// Resolves Alpaca's venue-generated order ID into the canonical
    /// Engine OrderId.
    pub fn get_by_venue_order_id(
        &self,
        venue_order_id: &str,
    ) -> Option<OrderId> {
        self.by_venue_order_id
            .get(venue_order_id)
            .copied()
    }


    // ============================================================
    // GET BY ORDER ID
    // ============================================================

    /// Resolves an Engine OrderId into Alpaca transport identifiers.
    pub fn get_by_order_id(
        &self,
        order_id: OrderId,
    ) -> Option<&AlpacaOrderRef> {
        self.by_order_id
            .get(&order_id)
    }


    // ============================================================
    // REMOVE BY ORDER ID
    // ============================================================

    /// Removes all indexes belonging to an order.
    ///
    /// Do not remove correlation merely because one terminal Alpaca update
    /// arrives. Late or duplicate trade_updates may still need correlation.
    /// Cleanup should eventually be controlled by an adapter reconciliation
    /// or retention policy.
    pub fn remove_by_order_id(
        &mut self,
        order_id: OrderId,
    ) -> Option<AlpacaOrderRef> {
        let order_ref =
            self.by_order_id.remove(
                &order_id
            )?;


        self.by_client_order_id.remove(
            &order_ref.client_order_id
        );


        if let Some(venue_order_id) =
            &order_ref.venue_order_id
        {
            self.by_venue_order_id.remove(
                venue_order_id
            );
        }


        Some(order_ref)
    }


    // ============================================================
    // LEN
    // ============================================================

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
    // ALPACA CORRELATION
    // ============================================================

    #[test]
    fn test_alpaca_order_correlation() {
        let order_id =
            OrderId::new(42);

        let client_order_id =
            ClientOrderId::new();

        let venue_order_id =
            "alpaca-order-123".to_string();


        let mut correlation =
            AlpacaOrderCorrelation::new();


        // --------------------------------------------------------
        // INSERT
        // --------------------------------------------------------

        correlation.insert(
            order_id,
            client_order_id,
        );


        assert_eq!(
            correlation.get_by_client_order_id(
                client_order_id
            ),
            Some(order_id)
        );


        let order_ref =
            correlation
                .get_by_order_id(
                    order_id
                )
                .expect(
                    "Expected Alpaca order correlation"
                );


        assert_eq!(
            order_ref.client_order_id,
            client_order_id
        );

        assert!(
            order_ref.venue_order_id.is_none()
        );


        // --------------------------------------------------------
        // ADD ALPACA VENUE ORDER ID
        // --------------------------------------------------------

        assert!(
            correlation.set_venue_order_id(
                order_id,
                venue_order_id.clone(),
            )
        );


        assert_eq!(
            correlation.get_by_venue_order_id(
                &venue_order_id
            ),
            Some(order_id)
        );


        let order_ref =
            correlation
                .get_by_order_id(
                    order_id
                )
                .expect(
                    "Expected Alpaca order correlation"
                );


        assert_eq!(
            order_ref.venue_order_id.as_deref(),
            Some("alpaca-order-123")
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
                    "Expected Alpaca correlation removal"
                );


        assert_eq!(
            removed.client_order_id,
            client_order_id
        );

        assert_eq!(
            removed.venue_order_id.as_deref(),
            Some("alpaca-order-123")
        );


        assert!(
            correlation
                .get_by_client_order_id(
                    client_order_id
                )
                .is_none()
        );

        assert!(
            correlation
                .get_by_venue_order_id(
                    &venue_order_id
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