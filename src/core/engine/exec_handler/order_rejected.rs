#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,
        execution::{
            OrderId,
            OrderPurpose,
            OrderState,
        },
        trades::TradeState,
        risk::TrailingStopState,
    },
    shared::print_log,
};


// OrderRejected
//     = venue explicitly refused the order
//     = canonical OrderState becomes Rejected
//     = remaining RiskReservation is released
//
// Entry rejection
//     = if the associated Trade is still Opening
//       and has received no entry fills, remove the Trade
//
// Exit-like rejection
//     = keep the Trade because actual exposure may still exist
//
// OrderRejected is different from ExecEvent::Error:
//
// OrderRejected
//     = venue explicitly said NO
//
// Error
//     = execution layer encountered a failure and the final
//       venue-side order state may still be unknown


// ============================================================
// HANDLE ORDER REJECTED
// ============================================================

impl Engine {
    /// Applies an explicit venue rejection to an existing canonical order.
    ///
    /// A rejection is a terminal execution fact. The venue has confirmed
    /// that the order will not continue working or receive future fills.
    ///
    /// This handler:
    ///
    /// - validates that the Order exists
    /// - prevents invalid terminal-state transitions
    /// - transitions the Order to `Rejected`
    /// - releases any remaining RiskReservation
    /// - removes an unfilled `Opening` Trade when its entry order failed
    ///
    /// Existing trades are NOT removed for rejected exit-like orders because
    /// the underlying position/exposure may still exist and require another
    /// exit attempt.
    pub fn handle_order_rejected(
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
                    "[ENGINE] ORDER REJECTED FOR UNKNOWN ORDER | order_id={} | reason={}",
                    order_id,
                    reason,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // A rejection for an unknown OrderId may indicate lost engine
            // state, broken event correlation, or reconciliation problems.

            return;
        };


        // ============================================================
        // TERMINAL STATE GUARD
        // ============================================================

        // Repeated rejection events for an already-rejected order are safe
        // and should be treated as idempotent.
        if order.state == OrderState::Rejected {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER ALREADY REJECTED | order_id={} | symbol={} | reason={}",
                    order.id,
                    order.symbol,
                    reason,
                ),
            );

            return;
        }


        // A different terminal state must never be overwritten by Rejected.
        //
        // Examples:
        //
        // Filled   -> Rejected   INVALID
        // Canceled -> Rejected   INVALID
        // Expired  -> Rejected   INVALID
        if order.state.is_terminal() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER REJECTION IGNORED | order_id={} | symbol={} | current_state={:?} | reason={}",
                    order.id,
                    order.symbol,
                    order.state,
                    reason,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // Conflicting terminal execution facts may indicate stale venue
            // events, incorrect event correlation, or reconciliation issues.

            return;
        }


        // ============================================================
        // CAPTURE ORDER METADATA
        // ============================================================

        // Capture the metadata needed below before mutating other parts of
        // EngineState. This also avoids holding an immutable Order borrow
        // while updating Orders, RiskReservations, and Trades.
        let trade_id =
            order.trade_id;

        let order_purpose =
            order.purpose;

        let symbol =
            order.symbol.clone();


        // ============================================================
        // TRANSITION TO REJECTED
        // ============================================================

        let updated =
            self.state.orders.set_state(
                order_id,
                OrderState::Rejected,
                ts_ms,
            );

        if !updated {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER REJECTED TRANSITION FAILED | order_id={} | symbol={} | reason={}",
                    order_id,
                    symbol,
                    reason,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in
            // live trading. The Order was validated immediately before this
            // transition, so failure indicates an engine-state inconsistency.

            return;
        }


        // ============================================================
        // RELEASE RISK RESERVATION
        // ============================================================

        // A rejected order can no longer consume future execution capacity.
        //
        // If the order had partially filled before rejection, only the
        // remaining reservation should exist at this point because each Fill
        // already reduced the reservation as execution occurred.
        self.state
            .risk
            .reservations
            .release(order_id);


        // ============================================================
        // HANDLE ASSOCIATED ENTRY TRADE
        // ============================================================

        // Only an Entry rejection may cause the logical Trade itself to be
        // removed.
        //
        // If an Entry order was rejected before receiving any fills, the
        // strategy never established exposure and the Opening Trade is no
        // longer needed.
        //
        // Exit, StopLoss, TakeProfit, and EmergencyExit rejections must NOT
        // remove the Trade because actual exposure may still remain.
        if order_purpose == OrderPurpose::Entry {
            if let Some(trade_id) = trade_id {
                let remove_trade =
                    match self.state.trades.get(trade_id) {
                        Some(trade) => {
                            trade.state == TradeState::Opening
                                && trade.quantity <= f64::EPSILON
                        }

                        None => {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[ENGINE] REJECTED ENTRY ORDER HAS UNKNOWN TRADE | order_id={} | trade_id={}",
                                    order_id,
                                    trade_id,
                                ),
                            );

                            // TODO:
                            // Send a remote Slack/Discord error notification
                            // in live trading. An Entry Order referencing a
                            // missing Trade indicates inconsistent engine state.

                            false
                        }
                    };


                if remove_trade {
                    self.state.trades.remove(
                        trade_id
                    );

                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] UNFILLED OPENING TRADE REMOVED AFTER ENTRY REJECTION | order_id={} | trade_id={} | symbol={}",
                            order_id,
                            trade_id,
                            symbol,
                        ),
                    );
                }

                // ============================================================
                // HANDLE PARTIALLY FILLED ENTRY REJECTION TRAILING STOP LOSS INIT
                // ============================================================

                // Confirmed partial fills represent real exposure.
                // Preserve the Trade and initialize trailing-stop protection
                // before transitioning it to Open.
                if let Some(trade) = self.state.trades.get_mut(trade_id) {
                    if trade.state == TradeState::Opening
                        && trade.quantity > f64::EPSILON
                    {
                        // ====================================================
                        // INITIALIZE TRAILING STOP
                        // ====================================================

                        if let Some(trailing_stop) = trade.trailing_stop.as_mut() {
                            if trailing_stop.best_px.is_none() {
                                if let Some(entry_px) = trade.avg_entry_px {
                                    match TrailingStopState::new(
                                        trailing_stop.config.clone(),
                                        trade.direction,
                                        entry_px,
                                    ) {
                                        Ok(state) => {
                                            *trailing_stop = state;
                                        }

                                        Err(err) => {
                                            // Confirmed fills have already updated
                                            // authoritative state and must be preserved.
                                            // TODO: Send a critical Slack/Discord alert.
                                            print_log(
                                                self.debug_mode,
                                                true,
                                                format!(
                                                    "[ENGINE] TRAILING STOP INITIALIZATION FAILED | trade_id={} | symbol={} | error={}",
                                                    trade.id,
                                                    trade.symbol,
                                                    err,
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        // ====================================================
                        // TRANSITION TRADE TO OPEN
                        // ====================================================

                        trade.state =
                            TradeState::Open;

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] PARTIALLY FILLED ENTRY REJECTED | order_id={} | trade_id={} | symbol={} | trade_qty={} | trade_state=Open",
                                order_id,
                                trade_id,
                                symbol,
                                trade.quantity,
                            ),
                        );
                    }
                }
            }
        }


        // ============================================================
        // LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] ORDER REJECTED | order_id={} | symbol={} | purpose={:?} | reason={}",
                order_id,
                symbol,
                order_purpose,
                reason,
            ),
        );
    }
}


// TODO/WARNING:
// Handle the rare case where an Entry order receives one or more confirmed
// partial fills and is then explicitly rejected by the venue.
//
// `handle_exec_fill()` will already have applied those confirmed fills to:
//
// - Order.filled_quantity
// - Trade.quantity
// - Trade.avg_entry_px
// - Trade.opened_ts_ms
// - Position
// - RiskReservation
//
// The current rejection handler correctly releases the remaining reservation,
// but only removes an Opening Trade when its established quantity is zero.
//
// If an Entry order is ever rejected AFTER a partial fill, the Trade may
// therefore remain:
//
//     TradeState::Opening
//         +
//     trade.quantity > 0
//
// even though real exposure has already been established and the rejected
// Order can no longer receive additional fills.
//
// This should eventually mirror the partial-fill handling used by
// `order_canceled.rs` / `order_expired.rs`:
//
//     Entry rejected with ZERO fills
//         -> remove the unfilled Opening Trade
//
//     Entry rejected AFTER PARTIAL fills
//         -> keep the established Trade
//         -> Opening -> Open
//         -> preserve the quantity and execution state already created
//            by the confirmed Fill(s)
//
// This is an execution-lifecycle edge case and does not need to block the
// initial Hyperliquid limit-order implementation.