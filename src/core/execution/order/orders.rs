use std::collections::HashMap;

use crate::core::{
    execution::order::{
        Order,
        OrderId,
        OrderState,
    },
    trades::TradeId,
};

/// Authoritative collection of execution orders known by the engine.
///
/// Orders are keyed by `OrderId`, not symbol, because multiple orders may
/// exist simultaneously for the same symbol, strategy, or trade.
///
/// This store represents the engine's canonical view of order lifecycle state.
/// Exchange adapters must not mutate it directly. They report `ExecEvent`s,
/// and engine handlers apply those events to this store.
#[derive(Debug, Default)]
pub struct Orders {
    orders: HashMap<OrderId, Order>,
}

impl Orders {
    /// Creates an empty order collection.
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
        }
    }

    /// Inserts a new order or replaces an existing order with the same ID.
    pub fn insert(&mut self, order: Order) -> Option<Order> {
        self.orders.insert(order.id, order)
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
                order.trade_id == Some(trade_id.as_u64())
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

    /// Removes an order from the collection.
    ///
    /// Normally terminal orders should remain until they have been persisted
    /// or archived rather than being removed immediately.
    pub fn remove(&mut self, order_id: OrderId) -> Option<Order> {
        self.orders.remove(&order_id)
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