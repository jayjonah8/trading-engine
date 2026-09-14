#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,

        execution::{
            ExecCommand,
            OrderChanges,
            OrderId,
            OrderPurpose,
            OrderState,
            OrderType,
            PendingOrderReplace,
        },

        risk::checks::{
            check_gross_notional,
            check_order_notional,
            check_symbol_notional,
        },
    },

    shared::print_log,
};


// ============================================================
// CANONICAL ORDER REPLACEMENT POLICY
// ============================================================

// Order replacement modifies execution parameters of an existing active
// canonical Order while preserving the engine-owned OrderId.
//
// Example:
//
// OrderId = 42
// BUY 10 BTC @ $100
//
// Replace request:
//
// OrderId = 42
// BUY 6 BTC @ $101
//
// The canonical OrderId remains 42 even if the venue internally performs
// the modification as cancel-and-replace and assigns a new venue_order_id.
//
// --------------------------------------------------------------------------
//
// Canonical order identity is IMMUTABLE:
//
// - OrderId
// - venue
// - strategy_id
// - trade_id
// - symbol
// - side
// - purpose
//
// Canonical replaceable execution parameters:
//
// - quantity
// - limit_px
// - stop_px
//
// --------------------------------------------------------------------------
//
// Replacement is asynchronous:
//
// Working
//     ↓
// handle_replace_order()
//     ↓
// validate requested changes
//     ↓
// risk-check proposed replacement
//     ↓
// store PendingOrderReplace
//     ↓
// OrderState::PendingReplace
//     ↓
// ExecCommand::ReplaceOrder
//     ↓
// execution router
//     ↓
// venue adapter
//     ↓
// venue confirms replacement
//     ↓
// ExecEvent::OrderReplaced
//     ↓
// exec_handler/order_replaced.rs
//     ↓
// apply stored changes
//     ↓
// clear pending_replace
//     ↓
// Working
//
// IMPORTANT:
//
// This request-side handler does NOT permanently modify quantity, limit_px,
// or stop_px. Those canonical fields change only after the venue confirms
// replacement through ExecEvent::OrderReplaced.
//
// For Entry Orders, replacement risk checks account for:
//
// established Position exposure
//     +
// existing Entry reservations
//     +
// additional risk introduced by this replacement
//
// The existing reservation for this Order must NOT be counted twice.


// ============================================================
// HANDLE REPLACE ORDER
// ============================================================

impl Engine {
    /// Requests modification of an existing active canonical Order.
    ///
    /// This is the request side of the asynchronous replacement lifecycle.
    pub fn handle_replace_order(
        &mut self,
        order_id: OrderId,
        changes: OrderChanges,
        reason: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // FIND ORDER
        // ============================================================

        let Some(order) = self.state.orders.get(order_id) else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER REJECTED | order_id={} | reason=unknown order",
                    order_id,
                ),
            );

            return;
        };


        // ============================================================
        // ORDER STATE
        // ============================================================

        if order.state != OrderState::Working {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | state={:?} | reason=order is not Working",
                    order.id,
                    order.symbol,
                    order.state,
                ),
            );

            return;
        }


        // ============================================================
        // EXISTING PENDING REPLACEMENT
        // ============================================================

        if order.pending_replace.is_some() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | reason=replacement already pending",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // EMPTY REPLACEMENT
        // ============================================================

        if changes.is_empty() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | reason=no replacement fields supplied",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // VALIDATE QUANTITY
        // ============================================================

        if let Some(quantity) = changes.quantity {
            if !quantity.is_finite() || quantity <= 0.0 {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | quantity={} | reason=invalid quantity",
                        order.id,
                        order.symbol,
                        quantity,
                    ),
                );

                return;
            }


            // Replacement quantity represents NEW TOTAL requested quantity.
            //
            // Example:
            //
            // original total = 10
            // filled         = 2
            // replacement    = 6
            //
            // new remaining = 4
            if quantity < order.filled_quantity {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | requested_qty={} | already_filled={} | reason=new total quantity is below filled quantity",
                        order.id,
                        order.symbol,
                        quantity,
                        order.filled_quantity,
                    ),
                );

                return;
            }


            if quantity <= order.filled_quantity + f64::EPSILON {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | requested_qty={} | filled_qty={} | reason=use cancellation to remove remaining quantity",
                        order.id,
                        order.symbol,
                        quantity,
                        order.filled_quantity,
                    ),
                );

                return;
            }
        }


        // ============================================================
        // VALIDATE PRICES
        // ============================================================

        if let Some(limit_px) = changes.limit_px {
            if !limit_px.is_finite() || limit_px <= 0.0 {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | limit_px={} | reason=invalid limit price",
                        order.id,
                        order.symbol,
                        limit_px,
                    ),
                );

                return;
            }
        }


        if let Some(stop_px) = changes.stop_px {
            if !stop_px.is_finite() || stop_px <= 0.0 {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | stop_px={} | reason=invalid stop price",
                        order.id,
                        order.symbol,
                        stop_px,
                    ),
                );

                return;
            }
        }


        // ============================================================
        // ORDER-TYPE FIELD VALIDATION
        // ============================================================

        match order.order_type {
            OrderType::Market => {
                if changes.limit_px.is_some() || changes.stop_px.is_some() {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | type=Market | reason=market order cannot replace limit/stop price",
                            order.id,
                            order.symbol,
                        ),
                    );

                    return;
                }
            }

            OrderType::Limit => {
                if changes.stop_px.is_some() {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | type=Limit | reason=limit order cannot replace stop price",
                            order.id,
                            order.symbol,
                        ),
                    );

                    return;
                }
            }

            OrderType::StopMarket => {
                if changes.limit_px.is_some() {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | type=StopMarket | reason=stop-market order cannot replace limit price",
                            order.id,
                            order.symbol,
                        ),
                    );

                    return;
                }
            }

            OrderType::StopLimit => {
                // quantity / limit_px / stop_px are all valid.
            }
        }


        // ============================================================
        // ACTUAL CHANGE CHECK
        // ============================================================

        let quantity_changed = changes.quantity
            .map(|value| (value - order.quantity).abs() > f64::EPSILON)
            .unwrap_or(false);

        let limit_changed = changes.limit_px
            .map(|value| {
                order.limit_px
                    .map(|current| (value - current).abs() > f64::EPSILON)
                    .unwrap_or(true)
            })
            .unwrap_or(false);

        let stop_changed = changes.stop_px
            .map(|value| {
                order.stop_px
                    .map(|current| (value - current).abs() > f64::EPSILON)
                    .unwrap_or(true)
            })
            .unwrap_or(false);


        if !quantity_changed && !limit_changed && !stop_changed {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | reason=request contains no actual changes",
                    order.id,
                    order.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // PROJECTED EXECUTION VALUES
        // ============================================================

        let projected_quantity = changes.quantity.unwrap_or(order.quantity);

        let projected_remaining_quantity =
            (projected_quantity - order.filled_quantity).max(0.0);

        let reference_px = changes.limit_px
            .or(order.limit_px)
            .or(changes.stop_px)
            .or(order.stop_px)
            .or_else(|| {
                self.last_px
                    .get(&order.symbol)
                    .map(|last| last.px)
            });


        // ============================================================
        // ENTRY EXPOSURE RISK CHECKS
        // ============================================================

        // Exit-like Orders reduce existing exposure and should not be blocked
        // by entry exposure limits.
        //
        // For now, these replacement exposure checks apply specifically to
        // directional Entry Orders.
        if order.purpose == OrderPurpose::Entry {
            let notional_limits_enabled =
                self.state.risk.limits.max_order_notional.is_some()
                    || self.state.risk.limits.max_gross_notional.is_some()
                    || self.state.risk.limits.max_symbol_notional.is_some();


            let reference_px =
                if notional_limits_enabled {
                    let Some(reference_px) = reference_px else {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | reason=no price available for replacement risk checks",
                                order.id,
                                order.symbol,
                            ),
                        );

                        return;
                    };

                    Some(reference_px)
                } else {
                    None
                };


            if let Some(reference_px) = reference_px {
                // ====================================================
                // MAX ORDER NOTIONAL
                // ====================================================

                // Per-order notional uses the replacement's NEW TOTAL quantity.
                let projected_order_notional =
                    projected_quantity * reference_px;


                if let Err(err) =
                    check_order_notional(
                        &self.state.risk.limits,
                        projected_order_notional,
                    )
                {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | projected_order_notional={} | error={:?}",
                            order.id,
                            order.symbol,
                            projected_order_notional,
                            err,
                        ),
                    );

                    return;
                }


                // ====================================================
                // PROJECTED REMAINING ENTRY NOTIONAL
                // ====================================================

                // Only unfilled quantity still represents reserved future
                // exposure.
                let projected_remaining_notional =
                    projected_remaining_quantity * reference_px;


                let existing_reserved_notional =
                    self.state
                        .risk
                        .reservations
                        .get(order_id)
                        .filter(|reservation| {
                            reservation.purpose
                                == crate::core::risk::ReservationPurpose::Entry
                        })
                        .map(|reservation| reservation.notional)
                        .unwrap_or(0.0);


                // check_gross_notional() already includes every active Entry
                // reservation, including this Order's CURRENT reservation.
                //
                // Therefore only add the replacement's INCREASE beyond the
                // currently reserved amount.
                let additional_entry_notional =
                    (projected_remaining_notional - existing_reserved_notional)
                        .max(0.0);


                // ====================================================
                // CURRENT GROSS POSITION NOTIONAL
                // ====================================================

                let current_gross_notional =
                    self.state
                        .positions
                        .iter()
                        .map(|(_, position)| {
                            let px =
                                self.last_px
                                    .get(&position.key.symbol)
                                    .map(|last| last.px)
                                    .or(position.avg_entry_px)
                                    .unwrap_or(0.0);

                            position.absolute_quantity() * px
                        })
                        .sum::<f64>();


                // ====================================================
                // MAX GROSS NOTIONAL
                // ====================================================

                if let Err(err) =
                    check_gross_notional(
                        &self.state.risk.limits,
                        &self.state.risk.reservations,
                        current_gross_notional,
                        additional_entry_notional,
                    )
                {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | additional_notional={} | error={:?}",
                            order.id,
                            order.symbol,
                            additional_entry_notional,
                            err,
                        ),
                    );

                    return;
                }


                // ====================================================
                // CURRENT SYMBOL POSITION NOTIONAL
                // ====================================================

                let current_symbol_notional =
                    self.state
                        .positions
                        .iter()
                        .filter(|(_, position)| {
                            position.key.symbol == order.symbol
                        })
                        .map(|(_, position)| {
                            let px =
                                self.last_px
                                    .get(&position.key.symbol)
                                    .map(|last| last.px)
                                    .or(position.avg_entry_px)
                                    .unwrap_or(reference_px);

                            position.absolute_quantity() * px
                        })
                        .sum::<f64>();


                // ====================================================
                // RESERVED SYMBOL NOTIONAL
                // ====================================================

                let reserved_symbol_notional =
                    self.state
                        .risk
                        .reservations
                        .entry_notional_for_symbol(
                            &order.symbol
                        );


                // ====================================================
                // MAX SYMBOL NOTIONAL
                // ====================================================

                if let Err(err) =
                    check_symbol_notional(
                        &self.state.risk.limits,
                        &order.symbol,
                        current_symbol_notional,
                        reserved_symbol_notional,
                        additional_entry_notional,
                    )
                {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | additional_notional={} | error={:?}",
                            order.id,
                            order.symbol,
                            additional_entry_notional,
                            err,
                        ),
                    );

                    return;
                }
            }
        }


        // ============================================================
        // CAPTURE ROUTING INFORMATION
        // ============================================================

        let venue = order.venue;
        let symbol = order.symbol.clone();


        // ============================================================
        // STORE PENDING REPLACEMENT
        // ============================================================

        // Do NOT modify canonical quantity / limit_px / stop_px yet.
        //
        // The venue has not confirmed the replacement.
        {
            let Some(order) = self.state.orders.get_mut(order_id) else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER STATE LOST BEFORE PENDING REPLACE | order_id={} | symbol={}",
                        order_id,
                        symbol,
                    ),
                );

                return;
            };

            order.pending_replace =
                Some(
                    PendingOrderReplace::new(
                        changes.clone(),
                        ts_ms,
                    )
                );

            order.state = OrderState::PendingReplace;
            order.updated_ts_ms = order.updated_ts_ms.max(ts_ms);
        }


        // ============================================================
        // SEND REPLACE COMMAND
        // ============================================================

        if let Err(err) =
            self.tx_exec.send(
                ExecCommand::ReplaceOrder {
                    venue,
                    order_id,
                    changes,
                }
            )
        {
            if let Some(order) = self.state.orders.get_mut(order_id) {
                order.pending_replace = None;
                order.state = OrderState::Working;
                order.updated_ts_ms = order.updated_ts_ms.max(ts_ms);
            }


            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER SEND FAILED | order_id={} | symbol={} | reason={} | error={}",
                    order_id,
                    symbol,
                    reason,
                    err,
                ),
            );

            return;
        }


        // ============================================================
        // REPLACEMENT REQUESTED
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] REPLACE ORDER REQUESTED | order_id={} | symbol={} | venue={:?} | quantity={:?} | limit_px={:?} | stop_px={:?} | reason={}",
                order_id,
                symbol,
                venue,
                self.state.orders
                    .get(order_id)
                    .and_then(|order| order.pending_replace.as_ref())
                    .and_then(|pending| pending.changes.quantity),
                self.state.orders
                    .get(order_id)
                    .and_then(|order| order.pending_replace.as_ref())
                    .and_then(|pending| pending.changes.limit_px),
                self.state.orders
                    .get(order_id)
                    .and_then(|order| order.pending_replace.as_ref())
                    .and_then(|pending| pending.changes.stop_px),
                reason,
            ),
        );
    }
}