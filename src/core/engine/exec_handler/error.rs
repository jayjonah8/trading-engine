#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,
        execution::OrderId,
    },
    shared::print_log,
};


// OrderRejected
//     = venue explicitly refused the order
//     = safe to transition Order -> Rejected
//
// Error
//     = execution / transport / SDK / adapter problem
//     = actual venue order state may be UNKNOWN
//     = DO NOT automatically change OrderState
//     = DO NOT release RiskReservation
//     = DO NOT remove Trade
//
// This distinction is critical.
//
// For example, an order submission request may time out after the request
// actually reached the venue. The adapter knows that communication failed,
// but it does NOT necessarily know whether the venue accepted the order.
//
// If the engine treated that Error as a rejection, released the reservation,
// and allowed another order to be submitted, both orders could potentially
// execute and create unintended duplicate exposure.
//
// Therefore:
//
// ExecEvent::Error
//     ↓
// log execution-layer failure
//     ↓
// preserve existing Order state
//     ↓
// preserve RiskReservation
//     ↓
// preserve Trade / Position state
//     ↓
// remote notification in live trading
//     ↓
// future reconciliation determines authoritative venue state


// TODO:
// Add explicit handling for authoritative order replacement rejection/failure.
//
// A confirmed replacement failure can safely roll back:
// - Order.pending_replace
// - OrderState::PendingReplace -> Working
// - any additional risk capacity reserved for the pending replacement
//
// Keep this separate from ExecEvent::Error, which is ambiguous and must
// preserve canonical state until venue reconciliation determines what happened.


// ============================================================
// HANDLE EXECUTION ERROR
// ============================================================

impl Engine {
    /// Handles an execution-layer error reported by an execution adapter.
    ///
    /// `ExecEvent::Error` represents an operational failure rather than an
    /// authoritative venue lifecycle fact.
    ///
    /// Examples include:
    ///
    /// - request timeout
    /// - transport/network failure
    /// - SDK failure
    /// - malformed/unexpected venue response
    /// - WebSocket processing failure
    /// - adapter-level execution failure
    ///
    /// Because the final venue-side state may be unknown, this handler does
    /// NOT automatically mutate Order, Trade, Position, FillLedger, or
    /// RiskReservation state.
    ///
    /// Future reconciliation/recovery logic can determine the authoritative
    /// venue state after an ambiguous execution failure.
    pub fn handle_exec_error(
        &mut self,
        order_id: Option<OrderId>,
        message: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // ORDER-ASSOCIATED ERROR
        // ============================================================

        if let Some(order_id) = order_id {
            let Some(order) =
                self.state.orders.get(order_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] EXECUTION ERROR FOR UNKNOWN ORDER | order_id={} | message={} | ts_ms={}",
                        order_id,
                        message,
                        ts_ms,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord error notification in live
                // trading. An execution error referencing an unknown OrderId
                // may indicate lost engine state, broken event correlation,
                // or a reconciliation problem.

                return;
            };


            // ============================================================
            // LOG ORDER ERROR
            // ============================================================

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] EXECUTION ERROR | order_id={} | venue_order_id={:?} | symbol={} | state={:?} | message={} | ts_ms={}",
                    order.id,
                    order.venue_order_id,
                    order.symbol,
                    order.state,
                    message,
                    ts_ms,
                ),
            );


            // ============================================================
            // PRESERVE AUTHORITATIVE STATE
            // ============================================================

            // IMPORTANT:
            //
            // Do NOT:
            //
            // - mark the Order as Rejected
            // - mark the Order as Canceled
            // - release its RiskReservation
            // - remove or close its Trade
            // - modify Position state
            //
            // The execution error does not prove what happened at the venue.
            //
            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            //
            // TODO:
            // Add execution reconciliation/recovery. For ambiguous failures,
            // query or otherwise reconcile venue state using venue_order_id,
            // client_order_id, or another stable correlation identifier.

            // TODO:
            // Add explicit handling for authoritative order replacement rejection/failure.
            //
            // A confirmed replacement failure can safely roll back:
            // - Order.pending_replace
            // - OrderState::PendingReplace -> Working
            // - any additional risk capacity reserved for the pending replacement
            //
            // Keep this separate from ExecEvent::Error, which is ambiguous and must
            // preserve canonical state until venue reconciliation determines what happened.



            return;
        }


        // ============================================================
        // GENERAL EXECUTION ERROR
        // ============================================================

        // Some execution-layer failures are not associated with a specific
        // Order. Examples include adapter connection failures, WebSocket
        // failures, authentication problems, or malformed global responses.
        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXECUTION ERROR | order_id=None | message={} | ts_ms={}",
                message,
                ts_ms,
            ),
        );


        // TODO:
        // Send a remote Slack/Discord error notification in live trading.
        //
        // TODO:
        // Add adapter/venue health and reconciliation handling for execution
        // errors that are not associated with one specific Order.
    }
}