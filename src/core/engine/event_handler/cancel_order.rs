#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,
        execution::{
            ExecCommand,
            OrderId,
            OrderState,
        },
    },
    shared::print_log,
};


// ============================================================
// CANCEL ORDER REQUEST FLOW
// ============================================================

// event_handler/cancel_order.rs
//     = "The engine WANTS this order canceled."
//
// exec_handler/order_canceled.rs
//     = "The venue CONFIRMED that the order is canceled."
//
// Cancellation is asynchronous:
//
// Working
//     ↓
// handle_cancel_order()
//     ↓
// validate order can be canceled
//     ↓
// OrderState::PendingCancel
//     ↓
// ExecCommand::CancelOrder
//     ↓
// execution router
//     ↓
// venue adapter
//     ↓
// venue confirms cancellation
//     ↓
// ExecEvent::OrderCanceled
//     ↓
// exec_handler/order_canceled.rs
//     ↓
// OrderState::Canceled
//     ↓
// release remaining RiskReservation
//
// IMPORTANT:
//
// PendingCancel does NOT mean the order is already canceled.
//
// The order may still receive fills while cancellation is in flight.
// Therefore this request-side handler must NOT:
//
// - release the RiskReservation
// - modify the Trade
// - modify the Position
// - remove the Order
//
// Confirmed Fill events remain authoritative until the venue confirms that
// the remaining order quantity has actually been canceled.


// ============================================================
// HANDLE CANCEL ORDER
// ============================================================

impl Engine {
    /// Requests cancellation of an existing active canonical order.
    ///
    /// This is the REQUEST side of the asynchronous cancellation lifecycle.
    ///
    /// The handler:
    ///
    /// - finds the canonical Order
    /// - validates that it can currently be canceled
    /// - transitions it to `PendingCancel`
    /// - sends `ExecCommand::CancelOrder` to the execution router
    /// - restores the previous state if the command cannot reach the router
    ///
    /// The actual terminal `Canceled` state is applied only after the venue
    /// confirms cancellation through `ExecEvent::OrderCanceled`.
    pub fn handle_cancel_order(
        &mut self,
        order_id: OrderId,
        reason: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // FIND ORDER
        // ============================================================

        let Some(order) =
            self.state.orders.get(order_id)
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CANCEL ORDER REJECTED | order_id={} | reason=unknown order | request_reason={}",
                    order_id,
                    reason,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading
            // when a cancellation request references an unknown OrderId.

            return;
        };


        // ============================================================
        // TERMINAL STATE GUARD
        // ============================================================

        // Terminal orders no longer have active quantity to cancel.
        //
        // Filled
        // Canceled
        // Rejected
        // Expired
        //
        // must never transition back into PendingCancel.
        if order.state.is_terminal() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CANCEL ORDER REJECTED | order_id={} | symbol={} | state={:?} | reason=order is terminal | request_reason={}",
                    order.id,
                    order.symbol,
                    order.state,
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // ALREADY PENDING CANCEL
        // ============================================================

        // Duplicate cancel requests are harmless, but do not send another
        // venue cancellation command while one is already outstanding.
        if order.state == OrderState::PendingCancel {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CANCEL ORDER IGNORED | order_id={} | symbol={} | reason=cancel already pending",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // PENDING REPLACEMENT GUARD
        // ============================================================

        // The first canonical implementation does not allow a cancellation
        // request to race an already-pending replacement.
        //
        // Supporting:
        //
        // PendingReplace -> PendingCancel
        //
        // requires explicit venue-independent semantics for determining which
        // asynchronous operation wins and which venue order ID is currently
        // authoritative.
        //
        // We can add that later if needed. For now, require the replacement
        // lifecycle to finish before requesting cancellation.
        if order.state == OrderState::PendingReplace
            || order.pending_replace.is_some()
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CANCEL ORDER REJECTED | order_id={} | symbol={} | reason=replacement currently pending",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // CANCELABLE STATE
        // ============================================================

        // For the initial implementation, only confirmed Working orders are
        // eligible for cancellation.
        //
        // PendingSubmit cancellation can be added later once we define how
        // every venue handles cancellation before working acknowledgement.
        if order.state != OrderState::Working {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CANCEL ORDER REJECTED | order_id={} | symbol={} | state={:?} | reason=order is not Working",
                    order.id,
                    order.symbol,
                    order.state,
                ),
            );

            return;
        }


        // ============================================================
        // CAPTURE ORDER METADATA
        // ============================================================

        // Capture routing/state information before mutating the Order.
        let venue =
            order.venue;

        let symbol =
            order.symbol.clone();

        let previous_state =
            order.state;


        // ============================================================
        // TRANSITION TO PENDING CANCEL
        // ============================================================

        {
            let Some(order) =
                self.state.orders.get_mut(order_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] CANCEL ORDER STATE LOST BEFORE PENDING CANCEL | order_id={} | symbol={}",
                        order_id,
                        symbol,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. The Order was validated immediately before
                // this point, so disappearance indicates engine-state
                // inconsistency.

                return;
            };

            order.state =
                OrderState::PendingCancel;

            order.updated_ts_ms =
                order.updated_ts_ms
                    .max(ts_ms);
        }


        // ============================================================
        // SEND CANCEL COMMAND
        // ============================================================

        // A successful channel send only means the execution router received
        // the request. The Order remains PendingCancel until the venue reports
        // ExecEvent::OrderCanceled.
        if let Err(err) =
            self.tx_exec.send(
                ExecCommand::CancelOrder {
                    venue,
                    order_id,
                }
            )
        {
            // ========================================================
            // ROLLBACK REQUEST STATE
            // ========================================================

            // The command never reached the execution router, so restore the
            // state that existed before this cancellation request.
            if let Some(order) =
                self.state.orders.get_mut(order_id)
            {
                order.state =
                    previous_state;

                order.updated_ts_ms =
                    order.updated_ts_ms
                        .max(ts_ms);
            }


            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CANCEL ORDER SEND FAILED | order_id={} | symbol={} | venue={:?} | reason={} | error={}",
                    order_id,
                    symbol,
                    venue,
                    reason,
                    err,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in live
            // trading when an execution command cannot reach the router.

            return;
        }


        // ============================================================
        // CANCELLATION REQUESTED
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] CANCEL ORDER REQUESTED | order_id={} | symbol={} | venue={:?} | state=PendingCancel | reason={}",
                order_id,
                symbol,
                venue,
                reason,
            ),
        );
    }
}