#![allow(dead_code)]

use std::time::{
    Instant,
    SystemTime,
};

use crate::{
    core::{
        engine::engine::Engine,
        execution::{
            OrderId,
            OrderPurpose,
            OrderState,
        },
        trades::TradeState,
    },
    shared::print_log,
};


// OrderExpired
//     = venue / time-in-force rules ended the remaining unfilled order
//     = canonical OrderState becomes Expired
//     = remaining RiskReservation is released
//
// Entry order expired with ZERO fills
//     ↓
// Order -> Expired
// release remaining RiskReservation
// remove Trade if it is still Opening with zero established quantity
//
// Entry order expired AFTER PARTIAL fills
//     ↓
// Order -> Expired
// release remaining RiskReservation
// KEEP Trade because real exposure exists
// Opening -> Open
// trade.quantity remains equal to the quantity actually filled
//
// Exit-like order expired
//     ↓
// Order -> Expired
// release remaining exit reservation
// KEEP Trade
// KEEP Position
//
// Any quantity that filled before expiration has already been applied by
// handle_exec_fill(). This handler only deals with the remaining unfilled
// portion and the resulting lifecycle state.


// ============================================================
// HANDLE ORDER EXPIRED
// ============================================================

impl Engine {
    /// Applies venue-confirmed expiration of an existing canonical order.
    ///
    /// Expiration is a terminal order lifecycle fact. The remaining unfilled
    /// quantity is no longer active because of venue or time-in-force rules.
    ///
    /// This handler:
    ///
    /// - validates that the Order exists
    /// - treats duplicate expiration events idempotently
    /// - prevents conflicting terminal-state transitions
    /// - transitions the Order to `Expired`
    /// - releases any remaining RiskReservation
    /// - cleans up an unfilled Opening entry Trade
    /// - promotes a partially filled Opening entry Trade to `Open`
    ///
    /// Existing Position state is never reverted here. Any confirmed fills
    /// that occurred before expiration remain authoritative.
    pub fn handle_order_expired(
        &mut self,
        order_id: OrderId,
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
                    "[ENGINE] ORDER EXPIRED FOR UNKNOWN ORDER | order_id={}",
                    order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // An expiration for an unknown OrderId may indicate lost state,
            // broken event correlation, or reconciliation problems.

            return;
        };


        // ============================================================
        // DUPLICATE EXPIRATION
        // ============================================================

        // Repeated expiration confirmations are harmless and should be
        // treated idempotently.
        if order.state == OrderState::Expired {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER ALREADY EXPIRED | order_id={} | symbol={}",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // TERMINAL STATE GUARD
        // ============================================================

        // A different terminal state must never be overwritten by Expired.
        //
        // Examples:
        //
        // Filled   -> Expired   INVALID
        // Canceled -> Expired   INVALID
        // Rejected -> Expired   INVALID
        if order.state.is_terminal() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER EXPIRATION IGNORED | order_id={} | symbol={} | current_state={:?}",
                    order.id,
                    order.symbol,
                    order.state,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // Conflicting terminal lifecycle facts may indicate stale venue
            // updates or incorrect execution-event correlation.

            return;
        }


        // ============================================================
        // CAPTURE ORDER METADATA
        // ============================================================

        // Capture metadata before mutating Orders so we do not keep an
        // immutable Order borrow while touching RiskState and Trades.
        let trade_id =
            order.trade_id;

        let order_purpose =
            order.purpose;

        let filled_quantity =
            order.filled_quantity;

        let symbol =
            order.symbol.clone();


        // ============================================================
        // TRANSITION TO EXPIRED
        // ============================================================

        let updated =
            self.state.orders.set_state(
                order_id,
                OrderState::Expired,
                ts_ms,
            );

        if !updated {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER EXPIRED TRANSITION FAILED | order_id={} | symbol={}",
                    order_id,
                    symbol,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in
            // live trading. The order was validated immediately before this
            // transition, so failure indicates an engine-state inconsistency.

            return;
        }


        // ============================================================
        // RELEASE REMAINING RISK RESERVATION
        // ============================================================

        // Each confirmed Fill already reduced the reservation by the executed
        // quantity. Expiration releases whatever remains reserved for the
        // unfilled portion of the order.
        self.state
            .risk
            .reservations
            .release(order_id);


        // ============================================================
        // HANDLE ASSOCIATED TRADE
        // ============================================================

        if let Some(trade_id) = trade_id {
            let Some(trade) =
                self.state.trades.get_mut(trade_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] EXPIRED ORDER HAS UNKNOWN TRADE | order_id={} | trade_id={}",
                        order_id,
                        trade_id,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord error notification in live
                // trading. An Order referencing a missing Trade indicates
                // inconsistent engine state.

                return;
            };


            match order_purpose {
                // ====================================================
                // ENTRY ORDER
                // ====================================================

                OrderPurpose::Entry => {
                    // ------------------------------------------------
                    // ENTRY EXPIRED WITH ZERO FILLS
                    // ------------------------------------------------

                    // No exposure was established, so the logical Opening
                    // Trade no longer represents anything real.
                    if filled_quantity <= f64::EPSILON
                        && trade.state == TradeState::Opening
                        && trade.quantity <= f64::EPSILON
                    {
                        self.state.trades.remove(
                            trade_id
                        );

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] UNFILLED OPENING TRADE REMOVED AFTER ENTRY EXPIRATION | order_id={} | trade_id={} | symbol={}",
                                order_id,
                                trade_id,
                                symbol,
                            ),
                        );

                        return;
                    }


                    // ------------------------------------------------
                    // ENTRY EXPIRED AFTER PARTIAL FILLS
                    // ------------------------------------------------

                    // Some entry quantity executed before expiration.
                    //
                    // handle_exec_fill() already updated:
                    //
                    // - Trade.quantity
                    // - Trade.avg_entry_px
                    // - Position
                    // - Order.filled_quantity
                    //
                    // The remaining entry quantity is no longer active, so
                    // the logical Trade becomes Open with only the quantity
                    // that actually executed.
                    if filled_quantity > f64::EPSILON
                        && trade.state == TradeState::Opening
                    {
                        trade.state =
                            TradeState::Open;

                        if trade.entry_time.is_none() {
                            trade.entry_time =
                                Some(
                                    Instant::now()
                                );
                        }

                        if trade.entry_wall_time.is_none() {
                            trade.entry_wall_time =
                                Some(
                                    SystemTime::now()
                                );
                        }

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] PARTIALLY FILLED ENTRY EXPIRED | order_id={} | trade_id={} | symbol={} | filled_qty={} | trade_qty={} | trade_state=Open",
                                order_id,
                                trade_id,
                                symbol,
                                filled_quantity,
                                trade.quantity,
                            ),
                        );
                    }
                }


                // ====================================================
                // EXIT-LIKE ORDERS
                // ====================================================

                OrderPurpose::Exit
                | OrderPurpose::StopLoss
                | OrderPurpose::TakeProfit
                | OrderPurpose::EmergencyExit => {
                    // Keep the Trade and Position.
                    //
                    // Any exit quantity that filled before expiration was
                    // already applied by handle_exec_fill().
                    //
                    // The remaining exposure may still require another exit
                    // attempt because this order is no longer active.
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] EXIT-LIKE ORDER EXPIRED | order_id={} | trade_id={} | symbol={} | purpose={:?} | filled_qty={} | remaining_trade_qty={}",
                            order_id,
                            trade_id,
                            symbol,
                            order_purpose,
                            filled_quantity,
                            trade.quantity,
                        ),
                    );
                }


                // ====================================================
                // NON-DIRECTIONAL ORDERS
                // ====================================================

                OrderPurpose::Quote
                | OrderPurpose::Hedge => {
                    // Quote/Hedge expiration does not directly change a
                    // directional Trade lifecycle.
                    //
                    // Any fills that occurred before expiration remain
                    // reflected in Position state.
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
                "[ENGINE] ORDER EXPIRED | order_id={} | symbol={} | purpose={:?} | filled_qty={}",
                order_id,
                symbol,
                order_purpose,
                filled_quantity,
            ),
        );
    }
}