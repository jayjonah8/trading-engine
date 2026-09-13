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


// ============================================================
// ORDER REPLACEMENT CONFIRMATION FLOW
// ============================================================

// event_handler/replace_order.rs
//     = "The engine WANTS to modify this order."
//
// exec_handler/order_replaced.rs
//     = "The venue CONFIRMED the modification."
//
// Replacement lifecycle:
//
// Working
//     ↓
// replacement requested
//     ↓
// PendingOrderReplace stored
//     ↓
// OrderState::PendingReplace
//     ↓
// ExecCommand::ReplaceOrder
//     ↓
// venue confirms replacement
//     ↓
// ExecEvent::OrderReplaced
//     ↓
// apply confirmed quantity / limit_px / stop_px
//     ↓
// update remaining RiskReservation
//     ↓
// clear pending_replace
//     ↓
// OrderState::Working
//
// The engine-owned OrderId never changes.
//
// A venue may preserve its existing venue_order_id or assign a new one.
// The confirmed `venue_order_id` therefore becomes the currently active
// venue-side identity for the canonical Order.
//
// --------------------------------------------------------------------------
//
// IMPORTANT:
//
// Some venues expose broader replace APIs that can technically modify fields
// such as side, order type, reduce-only behavior, or other venue-specific
// parameters.
//
// The canonical engine intentionally keeps order identity stable.
//
// Immutable canonical identity includes:
//
// - OrderId
// - venue
// - strategy_id
// - trade_id
// - symbol
// - side
// - purpose
//
// The current canonical replacement surface only allows:
//
// - quantity
// - limit_px
// - stop_px
//
// This keeps replacement semantics predictable across venues.


// ============================================================
// HANDLE ORDER REPLACED
// ============================================================

impl Engine {
    /// Applies venue confirmation that a pending order replacement succeeded.
    ///
    /// The requested replacement values were previously stored in
    /// `Order.pending_replace` by `handle_replace_order()`.
    ///
    /// This handler:
    ///
    /// - finds the canonical Order
    /// - verifies that a replacement was actually pending
    /// - applies the confirmed OrderChanges
    /// - updates the active venue-side order ID
    /// - adjusts the remaining RiskReservation
    /// - clears `pending_replace`
    /// - returns the Order to `Working`
    ///
    /// Trade, Position, and FillLedger state are not directly modified here.
    /// Any executions that occur while replacement is pending continue to
    /// arrive through `ExecEvent::Fill` and are handled by `fill.rs`.
    pub fn handle_order_replaced(
        &mut self,
        order_id: OrderId,
        venue_order_id: String,
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
                    "[ENGINE] ORDER REPLACED FOR UNKNOWN ORDER | order_id={} | venue_order_id={}",
                    order_id,
                    venue_order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // A replacement confirmation for an unknown OrderId may indicate
            // lost engine state, broken correlation, or reconciliation issues.

            return;
        };


        // ============================================================
        // PENDING REPLACEMENT
        // ============================================================

        // `pending_replace` is the authoritative record of what the engine
        // requested.
        //
        // Do not rely exclusively on OrderState::PendingReplace here because
        // a fill may legally arrive while replacement is in flight. fill.rs
        // may then move the Order back to Working or even Filled before the
        // replacement confirmation arrives.
        let Some(pending_replace) =
            order.pending_replace.clone()
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER REPLACED WITHOUT PENDING REQUEST | order_id={} | symbol={} | state={:?} | venue_order_id={}",
                    order.id,
                    order.symbol,
                    order.state,
                    venue_order_id,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // A replacement confirmation with no matching pending request may
            // indicate a stale venue event or incorrect event correlation.

            return;
        };


        // ============================================================
        // TERMINAL STATE GUARD
        // ============================================================

        // Never move a terminal Order back to Working.
        //
        // A terminal state may occur because an execution lifecycle event
        // raced with the replacement confirmation.
        //
        // Example:
        //
        // PendingReplace
        //     ↓
        // final Fill arrives
        //     ↓
        // Order -> Filled
        //     ↓
        // delayed OrderReplaced arrives
        //
        // In that case the terminal execution fact wins.
        if order.state.is_terminal() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER REPLACED IGNORED FOR TERMINAL ORDER | order_id={} | symbol={} | state={:?} | venue_order_id={}",
                    order.id,
                    order.symbol,
                    order.state,
                    venue_order_id,
                ),
            );

            // Clear the stale pending replacement so the canonical Order
            // does not permanently retain an operation that can no longer
            // affect an active order.
            if let Some(order) =
                self.state.orders.get_mut(order_id)
            {
                order.pending_replace = None;
                order.updated_ts_ms = order.updated_ts_ms.max(ts_ms);
            }

            // TODO:
            // Add execution reconciliation for replacement/fill races where
            // the venue confirms replacement after the order has already
            // reached a terminal state.

            return;
        };


        // ============================================================
        // CAPTURE CURRENT STATE
        // ============================================================

        let current_quantity =
            order.quantity;

        let filled_quantity =
            order.filled_quantity;

        let current_limit_px =
            order.limit_px;

        let current_stop_px =
            order.stop_px;

        let symbol =
            order.symbol.clone();


        // ============================================================
        // PROJECT CONFIRMED ORDER VALUES
        // ============================================================

        // Fields not included in OrderChanges preserve their existing value.
        let new_quantity =
            pending_replace
                .changes
                .quantity
                .unwrap_or(current_quantity);

        let new_limit_px =
            pending_replace
                .changes
                .limit_px
                .or(current_limit_px);

        let new_stop_px =
            pending_replace
                .changes
                .stop_px
                .or(current_stop_px);


        // ============================================================
        // REPLACEMENT / FILL RACE VALIDATION
        // ============================================================

        // Additional fills may arrive after the replacement request was sent
        // but before its confirmation is processed.
        //
        // Therefore the requested NEW TOTAL quantity must still be at least
        // as large as the quantity already executed.
        //
        // If this invariant is violated, do not invent canonical state.
        // Preserve the current Order and require reconciliation.
        if new_quantity + f64::EPSILON < filled_quantity {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER REPLACEMENT QUANTITY CONFLICT | order_id={} | symbol={} | replacement_qty={} | already_filled={}",
                    order_id,
                    symbol,
                    new_quantity,
                    filled_quantity,
                ),
            );

            if let Some(order) =
                self.state.orders.get_mut(order_id)
            {
                order.pending_replace = None;

                // Preserve the current active state rather than applying an
                // impossible total quantity.
                if !order.state.is_terminal() {
                    order.state = OrderState::Working;
                }

                order.updated_ts_ms =
                    order.updated_ts_ms.max(ts_ms);
            }

            // TODO:
            // Send a remote Slack/Discord critical error notification in live
            // trading and reconcile the authoritative venue Order state.

            return;
        }


        // ============================================================
        // REMAINING QUANTITY
        // ============================================================

        let new_remaining_quantity =
            (new_quantity - filled_quantity)
                .max(0.0);


        // ============================================================
        // RESERVATION NOTIONAL
        // ============================================================

        // RiskReservation.quantity represents REMAINING committed quantity,
        // not the total original Order quantity.
        //
        // Recompute approximate reservation notional using the best canonical
        // reference price currently available:
        //
        // 1. confirmed limit price
        // 2. confirmed stop price
        // 3. latest engine market price
        //
        // If no price is currently available, preserve the existing reserved
        // notional. The reservation quantity is still updated correctly.
        let reference_px =
            new_limit_px
                .or(new_stop_px)
                .or_else(|| {
                    self.last_px
                        .get(&symbol)
                        .map(|last| last.px)
                });

        let existing_reservation =
            self.state
                .risk
                .reservations
                .get(order_id)
                .cloned();

        let new_reserved_notional =
            match reference_px {
                Some(px) => {
                    new_remaining_quantity
                        * px
                }

                None => {
                    existing_reservation
                        .as_ref()
                        .map(|reservation| reservation.notional)
                        .unwrap_or(0.0)
                }
            };


        // ============================================================
        // APPLY CONFIRMED ORDER CHANGES
        // ============================================================

        {
            let Some(order) =
                self.state.orders.get_mut(order_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] ORDER DISAPPEARED BEFORE REPLACEMENT APPLY | order_id={} | symbol={}",
                        order_id,
                        symbol,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. The Order was validated immediately before
                // this point, so disappearance indicates state inconsistency.

                return;
            };


            order.quantity =
                new_quantity;

            order.limit_px =
                new_limit_px;

            order.stop_px =
                new_stop_px;

            // Some venues modify the existing venue-side order while others
            // internally cancel-and-replace and assign a new identifier.
            //
            // Always store the currently active confirmed venue order ID.
            order.venue_order_id =
                Some(
                    venue_order_id.clone()
                );

            order.pending_replace =
                None;

            order.updated_ts_ms =
                order.updated_ts_ms
                    .max(ts_ms);


            // If there is still unfilled quantity, the replacement is now
            // confirmed and the order returns to Working.
            //
            // A zero remaining quantity should normally have already resulted
            // in a Fill event and terminal Filled state. Do not manufacture a
            // Filled execution fact here.
            if new_remaining_quantity > f64::EPSILON {
                order.state =
                    OrderState::Working;
            }
        }


        // ============================================================
        // UPDATE RISK RESERVATION
        // ============================================================

        if new_remaining_quantity <= f64::EPSILON {
            // No future execution capacity remains to reserve.
            self.state
                .risk
                .reservations
                .release(order_id);
        } else if existing_reservation.is_some() {
            let updated =
                self.state
                    .risk
                    .reservations
                    .update(
                        order_id,
                        new_remaining_quantity,
                        new_reserved_notional,
                        ts_ms,
                    );

            if !updated {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] ORDER REPLACED RESERVATION UPDATE FAILED | order_id={} | symbol={} | remaining_qty={}",
                        order_id,
                        symbol,
                        new_remaining_quantity,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. An active replaced Order should not silently
                // lose its corresponding risk reservation.
            }
        } else {
            // An active order with remaining quantity would normally still
            // own a RiskReservation.
            //
            // Do not silently manufacture one here because the correct
            // ReservationPurpose / strategy / trade metadata belongs to the
            // original reservation lifecycle.
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER REPLACED WITHOUT RISK RESERVATION | order_id={} | symbol={} | remaining_qty={}",
                    order_id,
                    symbol,
                    new_remaining_quantity,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in live
            // trading and reconcile the missing reservation.
        }


        // ============================================================
        // LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] ORDER REPLACED | order_id={} | venue_order_id={} | symbol={} | quantity={} | filled_qty={} | remaining_qty={} | limit_px={:?} | stop_px={:?} | state=Working",
                order_id,
                venue_order_id,
                symbol,
                new_quantity,
                filled_quantity,
                new_remaining_quantity,
                new_limit_px,
                new_stop_px,
            ),
        );
    }
}