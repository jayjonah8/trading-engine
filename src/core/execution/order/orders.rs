#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::{
    execution::order::{
        ClientOrderId,
        Order,
        OrderId,
        OrderState,
    },
    trades::TradeId,
};


// One thing I would not add yet is venue_order_id -> OrderId as another index. 
// We can add that later for reconciliation/cancel/replace convenience if it becomes 
// useful, but CLOID gives us the cleaner primary correlation mechanism for incoming 
// Hyperliquid WS events.


/// Authoritative collection of execution orders known by the engine.
///
/// Orders are keyed by `OrderId`, not symbol, because multiple orders may
/// exist simultaneously for the same symbol, strategy, or trade.
///
/// This store represents the engine's canonical view of order lifecycle state.
/// Exchange adapters must not mutate it directly. They report `ExecEvent`s,
/// and engine handlers apply those events to this store.
#[derive(Debug, Clone, Default)]
pub struct Orders {
    /// Primary authoritative Order storage.
    ///
    /// Internal engine OrderId remains the main key used by core engine logic.
    orders: HashMap<OrderId, Order>,

    /// Secondary lookup from globally unique client-generated order identity
    /// back to the engine's canonical OrderId.
    ///
    /// This allows venue events carrying a client-order identifier to resolve
    /// directly to the correct canonical Order without relying on venue-specific
    /// correlation maps.
    ///
    /// Examples:
    ///
    /// Hyperliquid CLOID
    ///     -> ClientOrderId
    ///     -> OrderId
    ///
    /// Alpaca client_order_id
    ///     -> ClientOrderId
    ///     -> OrderId
    order_id_by_client_id: HashMap<ClientOrderId, OrderId>,
}

impl Orders {
    /// Creates an empty order collection.
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            order_id_by_client_id: HashMap::new(),
        }
    }

    /// Inserts a new Order or replaces an existing Order with the same OrderId.
    ///
    /// The canonical ClientOrderId secondary index is updated at the same time.
    ///
    /// ClientOrderId is expected to remain stable for the lifetime of an Order.
    /// If an Order with the same OrderId is replaced internally, any previous
    /// ClientOrderId mapping is removed before the new mapping is inserted.
    pub fn insert(
        &mut self,
        order: Order,
    ) -> Option<Order> {
        // ============================================================
        // REMOVE PREVIOUS SECONDARY INDEX
        // ============================================================

        if let Some(existing_order) =
            self.orders.get(&order.id)
        {
            self.order_id_by_client_id
                .remove(
                    &existing_order.client_order_id
                );
        }

        // ============================================================
        // INSERT CLIENT ORDER ID INDEX
        // ============================================================
        self.order_id_by_client_id
            .insert(
                order.client_order_id,
                order.id,
            );

        // ============================================================
        // INSERT ORDER
        // ============================================================
        self.orders.insert(
            order.id,
            order,
        )
    }

    /// Returns the canonical OrderId associated with a ClientOrderId.
    ///
    /// This is the primary bridge from venue-supplied client order identifiers
    /// back into the engine's internal Order identity.
    pub fn order_id_for_client_order_id(
        &self,
        client_order_id: ClientOrderId,
    ) -> Option<OrderId> {
        self.order_id_by_client_id
            .get(&client_order_id)
            .copied()
    }


    /// Returns an Order by its globally unique ClientOrderId.
    pub fn get_by_client_order_id(
        &self,
        client_order_id: ClientOrderId,
    ) -> Option<&Order> {
        let order_id =
            self.order_id_for_client_order_id(
                client_order_id
            )?;

        self.orders.get(
            &order_id
        )
    }


    /// Returns mutable access to an Order by ClientOrderId.
    pub fn get_mut_by_client_order_id(
        &mut self,
        client_order_id: ClientOrderId,
    ) -> Option<&mut Order> {
        let order_id =
            self.order_id_for_client_order_id(
                client_order_id
            )?;

        self.orders.get_mut(
            &order_id
        )
    }

    /// Returns an order by its internal engine ID.
    pub fn get(&self, order_id: OrderId) -> Option<&Order> {
        self.orders.get(&order_id)
    }

    /// Returns mutable access to an order.
    ///
    /// Engine execution-event handlers should be the primary users of
    /// mutable order access.
    pub fn get_mut(&mut self, order_id: OrderId) -> Option<&mut Order> {
        self.orders.get_mut(&order_id)
    }

    /// Returns true if an order exists.
    pub fn contains(&self, order_id: OrderId) -> bool {
        self.orders.contains_key(&order_id)
    }

    /// Returns true if at least one active order exists for the symbol.
    pub fn has_active_order_for_symbol(&self, symbol: &str) -> bool {
        self.orders
            .values()
            .any(|order| {
                order.symbol == symbol
                    && order.state.is_active()
            })
    }

    /// Returns the number of active orders.
    pub fn count_active(&self) -> usize {
        self.orders
            .values()
            .filter(|order| order.state.is_active())
            .count()
    }

    /// Returns all active orders associated with a trade.
    pub fn active_for_trade(
        &self,
        trade_id: TradeId,
    ) -> impl Iterator<Item = &Order> {
        self.orders
            .values()
            .filter(move |order| {
                order.trade_id == Some(trade_id)
                    && order.state.is_active()
            })
    }

    /// Updates an order lifecycle state.
    ///
    /// Returns false if the order does not exist.
    ///
    /// Terminal orders are prevented from transitioning back into an active
    /// state. More detailed transition validation can be added later.
    pub fn set_state(
        &mut self,
        order_id: OrderId,
        state: OrderState,
        ts_ms: i64,
    ) -> bool {
        let Some(order) = self.orders.get_mut(&order_id) else {
            return false;
        };

        if order.state.is_terminal() && state.is_active() {
            return false;
        }

        order.state = state;
        order.updated_ts_ms = ts_ms;

        true
    }

    /// Removes an Order and its ClientOrderId secondary index.
    ///
    /// Normally terminal Orders should remain available until persisted or
    /// archived rather than being removed immediately.
    pub fn remove(
        &mut self,
        order_id: OrderId,
    ) -> Option<Order> {
        let order =
            self.orders.remove(
                &order_id
            )?;

        self.order_id_by_client_id
            .remove(
                &order.client_order_id
            );

        Some(order)
    }

    /// Returns the total number of stored order records.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// Returns true when no order records exist.
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// Iterates over all known orders.
    pub fn iter(&self) -> impl Iterator<Item = (&OrderId, &Order)> {
        self.orders.iter()
    }
}