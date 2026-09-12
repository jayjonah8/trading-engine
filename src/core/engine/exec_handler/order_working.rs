#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,
        execution::{
            OrderId,
            OrderState,
        },
    },
    shared::print_log,
};


// OrderAccepted
//     = venue acknowledged the order
//     = venue_order_id is now known
//     = does NOT necessarily mean the order is Working
//
// OrderWorking
//     = order is actually resting/active at the venue
//     = canonical OrderState becomes Working
//     = capable of receiving future fills
//
// Fill
//     = execution actually happened
//     = changes Order / Position / Trade / Risk state


// ============================================================
// HANDLE ORDER WORKING
// ============================================================

impl Engine {
    /// Applies confirmation that an existing canonical order is now actively
    /// working at the execution venue.
    ///
    /// `OrderWorking` means the venue has confirmed that the order is active
    /// and capable of receiving future fills.
    ///
    /// Typical lifecycle:
    ///
    /// PendingSubmit
    ///     -> OrderAccepted
    ///     -> OrderWorking
    ///     -> Working
    ///
    /// This handler only updates canonical `Order` lifecycle state.
    /// It does NOT modify:
    ///
    /// - Trade state
    /// - Position state
    /// - FillLedger
    /// - Risk reservations
    ///
    /// Those change only when their corresponding execution facts occur.
    pub fn handle_order_working(
        &mut self,
        order_id: OrderId,
        ts_ms: i64,
    ) {
        // ============================================================
        // FIND ORDER
        // ============================================================

        let Some(order) =
            self.state.orders.get(
                order_id
            )
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER WORKING FOR UNKNOWN ORDER | order_id={}",
                    order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // Receiving a Working event for an unknown OrderId may indicate
            // stale execution events, broken correlation, or state loss.

            return;
        };


        // ============================================================
        // TERMINAL STATE GUARD
        // ============================================================

        // A terminal order must never transition back into an active state.
        //
        // Examples:
        //
        // Filled   -> Working   INVALID
        // Canceled -> Working   INVALID
        // Rejected -> Working   INVALID
        // Expired  -> Working   INVALID
        //
        // `Orders::set_state()` also protects this invariant, but checking
        // here lets us produce a clear execution-lifecycle log.
        if order.state.is_terminal() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER WORKING IGNORED | order_id={} | symbol={} | current_state={:?} | reason=terminal order cannot return to working",
                    order.id,
                    order.symbol,
                    order.state,
                ),
            );

            return;
        }


        // ============================================================
        // ALREADY WORKING
        // ============================================================

        // Duplicate Working events are harmless.
        //
        // WebSocket reconnects, polling, or repeated venue updates may report
        // the same lifecycle fact more than once, so this handler is
        // intentionally idempotent.
        if order.state == OrderState::Working {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER ALREADY WORKING | order_id={} | symbol={}",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // TRANSITION TO WORKING
        // ============================================================

        // `set_state()` updates both:
        //
        // - order.state
        // - order.updated_ts_ms
        //
        // and prevents terminal -> active transitions.
        let updated =
            self.state.orders.set_state(
                order_id,
                OrderState::Working,
                ts_ms,
            );


        if !updated {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER WORKING TRANSITION FAILED | order_id={}",
                    order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // A failed Working transition after the order was already validated
            // suggests an internal engine-state inconsistency.

            return;
        }


        // ============================================================
        // VERIFY UPDATED ORDER
        // ============================================================

        let Some(order) =
            self.state.orders.get(
                order_id
            )
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER DISAPPEARED AFTER WORKING TRANSITION | order_id={}",
                    order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in
            // live trading. An order disappearing after a successful state
            // transition indicates an internal engine-state invariant failure.
            //
            // Do not panic here. Keep the process alive so the bot can surface
            // the error and potentially reconcile state.

            return;
        };


        // ============================================================
        // LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] ORDER WORKING | order_id={} | venue_order_id={:?} | symbol={} | state={:?}",
                order.id,
                order.venue_order_id,
                order.symbol,
                order.state,
            ),
        );
    }
}