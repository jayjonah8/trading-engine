#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,
        execution::OrderId,
    },
    shared::print_log,
};

impl Engine {
    /// Applies a venue acknowledgement to an existing canonical order.
    ///
    /// `OrderAccepted` records the venue-side order identifier but does not
    /// transition the order to `Working`.
    ///
    /// The order remains `PendingSubmit` until a later execution fact proves
    /// that it is:
    ///
    /// - `Working`
    /// - `Filled`
    /// - `Rejected`
    /// - canceled / expired
    ///
    /// Keeping acceptance separate from working state allows immediate market
    /// fills and resting orders to share the same canonical lifecycle.
    pub fn handle_order_accepted(
        &mut self,
        order_id: OrderId,
        venue_order_id: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // FIND ORDER
        // ============================================================

        let Some(order) =
            self.state.orders.get_mut(
                order_id
            )
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER ACCEPTED FOR UNKNOWN ORDER | order_id={} | venue_order_id={}",
                    order_id,
                    venue_order_id,
                ),
            );

            return;
        };

        // ============================================================
        // VENUE ORDER ID
        // ============================================================

        match &order.venue_order_id {
            Some(existing_id)
                if existing_id != &venue_order_id =>
            {
                // Do not silently replace an already-known venue identity.
                // A conflicting venue order ID may indicate an adapter,
                // reconciliation, or event-correlation problem.
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] ORDER ACCEPTED ID CONFLICT | order_id={} | existing_venue_order_id={} | received_venue_order_id={}",
                        order_id,
                        existing_id,
                        venue_order_id,
                    ),
                );

                return;
            }

            Some(_) => {
                // Duplicate acknowledgement for the same venue order.
                // This is safe and intentionally idempotent.
            }

            None => {
                order.venue_order_id =
                    Some(
                        venue_order_id.clone()
                    );
            }
        }

        // Preserve monotonic lifecycle time if venue events arrive
        // slightly out of order.
        order.updated_ts_ms =
            order.updated_ts_ms
                .max(ts_ms);

        // ============================================================
        // LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] ORDER ACCEPTED | order_id={} | venue_order_id={} | symbol={} | state={:?}",
                order.id,
                venue_order_id,
                order.symbol,
                order.state,
            ),
        );
    }
}