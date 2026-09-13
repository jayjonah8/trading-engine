#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,

        execution::{
            ExecCommand,
            OrderChanges,
            OrderId,
            OrderState,
            OrderType,
            PendingOrderReplace,
        },

        risk::checks::check_order_notional,
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
// Canonical order identity is intentionally IMMUTABLE:
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
// Some venues may support broader modifications, including fields such as
// side, order type, reduce-only, or other venue-specific parameters.
//
// The core engine intentionally exposes a smaller, safer common subset.
// Venue-specific capabilities must not leak into canonical engine semantics.
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
// risk-check proposed order
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
// or stop_px. Those canonical fields are changed only after the venue
// confirms the replacement through ExecEvent::OrderReplaced.


// TODO:
// When max_gross_notional and max_symbol_notional risk checks are implemented,
// replacement requests that increase exposure must reserve the additional
// risk capacity while PendingReplace is outstanding.
//
// The current first version performs the available per-order notional check.
// Full pending-replacement risk-delta reservation will be added alongside
// canonical gross/symbol exposure accounting.


// ============================================================
// HANDLE REPLACE ORDER
// ============================================================

impl Engine {
    /// Requests modification of an existing active canonical order.
    ///
    /// This is the REQUEST side of the asynchronous replacement lifecycle.
    ///
    /// It validates the proposed changes, stores them in `pending_replace`,
    /// transitions the order to `PendingReplace`, and sends the canonical
    /// `ExecCommand::ReplaceOrder` to the execution router.
    ///
    /// The actual Order fields are not changed here. They are applied only
    /// after `ExecEvent::OrderReplaced` confirms the venue accepted them.
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

        let Some(order) =
            self.state.orders.get(order_id)
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] REPLACE ORDER REJECTED | order_id={} | reason=unknown order",
                    order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading
            // when replacement requests reference unknown canonical orders.

            return;
        };


        // ============================================================
        // ORDER STATE
        // ============================================================

        // For the initial canonical implementation, replacement is only
        // allowed for an order that is actively Working.
        //
        // Do not replace:
        //
        // PendingSubmit
        // PendingCancel
        // PendingReplace
        // Filled
        // Canceled
        // Rejected
        // Expired
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

        // Only one replacement may be outstanding for an Order at a time.
        //
        // Allowing multiple asynchronous replacements before confirmation
        // would make it ambiguous which requested changes a venue response
        // actually confirmed.
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
            if !quantity.is_finite()
                || quantity <= 0.0
            {
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


            // Replacement quantity represents NEW TOTAL requested quantity,
            // not remaining quantity.
            //
            // Example:
            //
            // original total = 10
            // already filled = 2
            // replacement    = 6
            //
            // new remaining = 6 - 2 = 4
            //
            // The new total therefore cannot be less than quantity that has
            // already executed.
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


            // Setting total quantity exactly equal to the quantity already
            // filled would leave zero remaining quantity.
            //
            // Use CancelOrder to remove the remaining working quantity instead
            // of encoding cancellation as a replacement.
            if quantity <= order.filled_quantity
                + f64::EPSILON
            {
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
            if !limit_px.is_finite()
                || limit_px <= 0.0
            {
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
            if !stop_px.is_finite()
                || stop_px <= 0.0
            {
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

        // Canonical replacement fields must make sense for the OrderType.
        //
        // Market
        //     quantity only
        //
        // Limit
        //     quantity / limit_px
        //
        // StopMarket
        //     quantity / stop_px
        //
        // StopLimit
        //     quantity / limit_px / stop_px
        match order.order_type {
            OrderType::Market => {
                if changes.limit_px.is_some()
                    || changes.stop_px.is_some()
                {
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
                // Quantity, limit price, and stop price are all valid
                // replacement parameters for a canonical StopLimit order.
            }
        }


        // ============================================================
        // ACTUAL CHANGE CHECK
        // ============================================================

        // Reject requests that technically contain fields but would not change
        // the canonical Order at all.
        let quantity_changed =
            changes.quantity
                .map(|value| {
                    (value - order.quantity).abs()
                        > f64::EPSILON
                })
                .unwrap_or(false);

        let limit_changed =
            changes.limit_px
                .map(|value| {
                    order.limit_px
                        .map(|current| {
                            (value - current).abs()
                                > f64::EPSILON
                        })
                        .unwrap_or(true)
                })
                .unwrap_or(false);

        let stop_changed =
            changes.stop_px
                .map(|value| {
                    order.stop_px
                        .map(|current| {
                            (value - current).abs()
                                > f64::EPSILON
                        })
                        .unwrap_or(true)
                })
                .unwrap_or(false);


        if !quantity_changed
            && !limit_changed
            && !stop_changed
        {
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
        // PROJECTED ORDER NOTIONAL
        // ============================================================

        // Replacement can increase risk even when quantity does not change.
        //
        // Example:
        //
        // BUY 10 @ $100
        //     -> replace
        // BUY 10 @ $120
        //
        // The quantity is unchanged but requested notional increased.
        //
        // Use the best canonical reference price currently available:
        //
        // 1. proposed limit price
        // 2. existing limit price
        // 3. proposed stop price
        // 4. existing stop price
        // 5. latest engine market price
        let projected_quantity =
            changes.quantity
                .unwrap_or(order.quantity);

        let reference_px =
            changes.limit_px
                .or(order.limit_px)
                .or(changes.stop_px)
                .or(order.stop_px)
                .or_else(|| {
                    self.last_px
                        .get(&order.symbol)
                        .map(|last| last.px)
                });


        // Only perform this calculation when the engine currently has a
        // max-order-notional limit configured.
        //
        // If no such limit exists, no per-order notional gate is required.
        if self.state.risk.limits.max_order_notional.is_some() {
            let Some(reference_px) =
                reference_px
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | reason=no price available for order-notional risk check",
                        order.id,
                        order.symbol,
                    ),
                );

                return;
            };


            let projected_notional =
                projected_quantity
                    * reference_px;


            if let Err(err) =
                check_order_notional(
                    &self.state.risk.limits,
                    projected_notional,
                )
            {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER REJECTED | order_id={} | symbol={} | projected_notional={} | error={:?}",
                        order.id,
                        order.symbol,
                        projected_notional,
                        err,
                    ),
                );

                return;
            }
        }


        // ============================================================
        // CAPTURE ROUTING INFORMATION
        // ============================================================

        let venue =
            order.venue;

        let symbol =
            order.symbol.clone();


        // ============================================================
        // STORE PENDING REPLACEMENT
        // ============================================================

        // Do NOT change quantity / limit_px / stop_px here.
        //
        // The venue has not confirmed anything yet.
        //
        // Store exactly what the engine requested so OrderReplaced can apply
        // the confirmed changes later without relying on a venue-specific
        // response to echo every field.
        {
            let Some(order) =
                self.state.orders.get_mut(order_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] REPLACE ORDER STATE LOST BEFORE PENDING REPLACE | order_id={} | symbol={}",
                        order_id,
                        symbol,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in live
                // trading. The order was validated immediately before this point,
                // so disappearing here indicates an internal engine-state inconsistency.

                return;
            };

            order.pending_replace =
                Some(
                    PendingOrderReplace::new(
                        changes.clone(),
                        ts_ms,
                    )
                );

            order.state =
                OrderState::PendingReplace;

            order.updated_ts_ms =
                order.updated_ts_ms
                    .max(ts_ms);
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
            // The command never reached the execution router.
            //
            // Since the replacement request was not handed off, restore the
            // canonical Order to Working and remove the pending replacement.
            if let Some(order) =
                self.state.orders.get_mut(order_id)
            {
                order.pending_replace =
                    None;

                order.state =
                    OrderState::Working;

                order.updated_ts_ms =
                    order.updated_ts_ms
                        .max(ts_ms);
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

            // TODO:
            // Send a remote Slack/Discord critical error notification in
            // live trading when an execution command cannot reach the router.

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
                    .and_then(|order| {
                        order.pending_replace
                            .as_ref()
                    })
                    .and_then(|pending| {
                        pending.changes.quantity
                    }),
                self.state.orders
                    .get(order_id)
                    .and_then(|order| {
                        order.pending_replace
                            .as_ref()
                    })
                    .and_then(|pending| {
                        pending.changes.limit_px
                    }),
                self.state.orders
                    .get(order_id)
                    .and_then(|order| {
                        order.pending_replace
                            .as_ref()
                    })
                    .and_then(|pending| {
                        pending.changes.stop_px
                    }),
                reason,
            ),
        );
    }
}