#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::{
    execution::order::OrderId,
    risk::reservation::{
        ReservationPurpose,
        RiskReservation,
    },
    trades::TradeId,
};

/// Authoritative collection of active risk reservations.
///
/// Reservations are keyed by `OrderId` because each working order owns its
/// own committed risk capacity.
///
/// This prevents pending execution from being invisible to risk checks.
///
/// For example:
///
/// - A working entry order reserves future exposure before it fills.
/// - A working exit order reserves existing trade quantity so another exit
///   cannot reserve the same quantity at the same time.
#[derive(Debug, Default)]
pub struct RiskReservations {
    reservations: HashMap<OrderId, RiskReservation>,
}

impl RiskReservations {
    /// Creates an empty reservation collection.
    pub fn new() -> Self {
        Self {
            reservations: HashMap::new(),
        }
    }

    /// Inserts a new reservation.
    ///
    /// Returns the previous reservation if one already existed for the same
    /// order ID.
    pub fn insert(
        &mut self,
        reservation: RiskReservation,
    ) -> Option<RiskReservation> {
        self.reservations
            .insert(reservation.order_id, reservation)
    }

    /// Returns a reservation by its owning order ID.
    pub fn get(
        &self,
        order_id: OrderId,
    ) -> Option<&RiskReservation> {
        self.reservations.get(&order_id)
    }

    /// Returns mutable access to a reservation.
    pub fn get_mut(
        &mut self,
        order_id: OrderId,
    ) -> Option<&mut RiskReservation> {
        self.reservations.get_mut(&order_id)
    }

    /// Completely releases the reservation owned by an order.
    ///
    /// Normally used when the order is canceled, rejected, expired, or no
    /// longer has any remaining quantity requiring reservation.
    pub fn release(
        &mut self,
        order_id: OrderId,
    ) -> Option<RiskReservation> {
        self.reservations.remove(&order_id)
    }

    /// Reduces a reservation after part of an order fills.
    ///
    /// The reservation is automatically removed when its remaining quantity
    /// reaches zero.
    ///
    /// TODO: Update reserved notional proportionally or recompute it using
    /// canonical `Price`, `Quantity`, and `Money` types once those are added.
    pub fn reduce_quantity(
        &mut self,
        order_id: OrderId,
        filled_quantity: f64,
        ts_ms: i64,
    ) -> bool {
        let Some(reservation) = self.reservations.get_mut(&order_id) else {
            return false;
        };

        reservation.quantity =
            (reservation.quantity - filled_quantity).max(0.0);

        reservation.updated_ts_ms = ts_ms;

        if reservation.quantity == 0.0 {
            self.reservations.remove(&order_id);
        }

        true
    }

    /// Returns the total quantity reserved by entry orders for a symbol.
    pub fn entry_quantity_for_symbol(
        &self,
        symbol: &str,
    ) -> f64 {
        self.reservations
            .values()
            .filter(|reservation| {
                reservation.symbol == symbol
                    && reservation.purpose == ReservationPurpose::Entry
            })
            .map(|reservation| reservation.quantity)
            .sum()
    }

    /// Returns the total quantity reserved by exit orders for a trade.
    pub fn exit_quantity_for_trade(
        &self,
        trade_id: TradeId,
    ) -> f64 {
        self.reservations
            .values()
            .filter(|reservation| {
                reservation.trade_id == Some(trade_id)
                    && reservation.purpose == ReservationPurpose::Exit
            })
            .map(|reservation| reservation.quantity)
            .sum()
    }

    /// Returns total notional reserved by all active entry orders.
    pub fn total_entry_notional(&self) -> f64 {
        self.reservations
            .values()
            .filter(|reservation| {
                reservation.purpose == ReservationPurpose::Entry
            })
            .map(|reservation| reservation.notional)
            .sum()
    }

    /// Returns true if the given order currently owns a reservation.
    pub fn contains(
        &self,
        order_id: OrderId,
    ) -> bool {
        self.reservations.contains_key(&order_id)
    }

    /// Returns the number of active reservations.
    pub fn len(&self) -> usize {
        self.reservations.len()
    }

    /// Returns true when no active reservations exist.
    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
    }

    /// Iterates over all active reservations.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&OrderId, &RiskReservation)> {
        self.reservations.iter()
    }
}