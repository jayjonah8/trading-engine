#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,

        execution::{
            Fill,
            OrderPurpose,
            OrderState,
            Side,
        },

        positions::PositionKey,

        trades::TradeState,
    },

    shared::print_log,
};


// ============================================================
// HANDLE FILL
// ============================================================

impl Engine {
    /// Applies one confirmed execution to authoritative engine state.
    ///
    /// Fill
    ///     -> validate against Order
    ///     -> validate associated Trade
    ///     -> deduplicate through FillLedger
    ///     -> update Order execution state
    ///     -> reduce RiskReservation
    ///     -> update Position
    ///     -> update associated Trade
    ///
    /// A Fill is an authoritative execution fact that changes actual exposure.
    ///
    /// Submitting, accepting, working, canceling, or replacing an Order does
    /// not itself change Position or Trade quantity. Confirmed fills do.
    ///
    /// Trade timing is based on canonical event timestamps:
    ///
    ///     trade.opened_ts_ms = first confirmed entry fill timestamp
    ///
    /// Risk logic later compares market-event `ts_ms` against `opened_ts_ms`.
    /// This keeps timing deterministic across:
    ///
    /// - live trading
    /// - simulation
    /// - historical backtesting
    pub fn handle_exec_fill(
        &mut self,
        fill: Fill,
    ) {
        // ============================================================
        // VALIDATE FILL
        // ============================================================

        if !fill.quantity.is_finite()
            || fill.quantity <= 0.0
            || !fill.price.is_finite()
            || fill.price <= 0.0
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] INVALID FILL | order_id={} | symbol={} | qty={} | price={}",
                    fill.order_id,
                    fill.symbol,
                    fill.quantity,
                    fill.price,
                ),
            );

            return;
        }


        // ============================================================
        // VALIDATE AGAINST ORDER
        // ============================================================

        let Some(order) =
            self.state.orders.get(fill.order_id)
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] FILL FOR UNKNOWN ORDER | order_id={} | symbol={}",
                    fill.order_id,
                    fill.symbol,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading.
            // A Fill referencing an unknown OrderId may indicate lost state,
            // broken execution correlation, or reconciliation problems.

            return;
        };


        if order.venue != fill.venue
            || order.symbol != fill.symbol
            || order.side != fill.side
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] FILL/ORDER MISMATCH | order_id={} | order_symbol={} | fill_symbol={} | order_side={:?} | fill_side={:?} | order_venue={:?} | fill_venue={:?}",
                    fill.order_id,
                    order.symbol,
                    fill.symbol,
                    order.side,
                    fill.side,
                    order.venue,
                    fill.venue,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in
            // live trading. Applying a mismatched execution could corrupt
            // authoritative exposure.

            return;
        }


        // ============================================================
        // OVERFILL GUARD
        // ============================================================

        let remaining_quantity =
            order.remaining_quantity();

        if fill.quantity > remaining_quantity + f64::EPSILON {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] ORDER OVERFILL REJECTED | order_id={} | fill_qty={} | remaining_qty={}",
                    fill.order_id,
                    fill.quantity,
                    remaining_quantity,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification and
            // reconcile the venue order. A confirmed venue fill exceeding
            // canonical remaining quantity indicates state divergence.

            return;
        }


        // Capture metadata before mutating the Order.
        let trade_id =
            order.trade_id;

        let order_purpose =
            order.purpose;

        let previous_order_state =
            order.state;


        // ============================================================
        // VALIDATE ASSOCIATED TRADE
        // ============================================================

        if let Some(trade_id) = trade_id {
            if self.state.trades.get(trade_id).is_none() {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] FILL ORDER HAS UNKNOWN TRADE | order_id={} | trade_id={}",
                        fill.order_id,
                        trade_id,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. An Order associated with a missing Trade
                // indicates inconsistent engine state.

                return;
            }
        }


        // ============================================================
        // DEDUPLICATE FILL
        // ============================================================

        // Record the execution BEFORE applying state mutations.
        //
        // If the same execution is observed again through an immediate API
        // response, WebSocket update, reconnect, polling, or reconciliation,
        // it must never modify engine state twice.
        if !self.state.fills.insert(
            fill.clone()
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] DUPLICATE FILL IGNORED | execution_id={:?} | order_id={}",
                    fill.execution_id,
                    fill.order_id,
                ),
            );

            return;
        }


        // ============================================================
        // UPDATE ORDER
        // ============================================================

        let order_fully_filled;

        {
            let Some(order) =
                self.state.orders.get_mut(fill.order_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] ORDER DISAPPEARED BEFORE FILL APPLY | order_id={} | symbol={}",
                        fill.order_id,
                        fill.symbol,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. The Order was validated immediately before
                // this point, so disappearance indicates state inconsistency.

                return;
            };


            let old_filled_quantity =
                order.filled_quantity;

            let new_filled_quantity =
                old_filled_quantity
                    + fill.quantity;


            // Recalculate order-level execution VWAP.
            order.avg_fill_px =
                Some(
                    weighted_average(
                        order.avg_fill_px,
                        old_filled_quantity,
                        fill.price,
                        fill.quantity,
                    )
                );


            order.filled_quantity =
                new_filled_quantity;

            order.updated_ts_ms =
                order.updated_ts_ms
                    .max(fill.ts_ms);


            order_fully_filled =
                order.remaining_quantity()
                    <= f64::EPSILON;


            if order_fully_filled {
                // A complete execution is terminal regardless of whether
                // cancellation or replacement was previously pending.
                order.state =
                    OrderState::Filled;
            } else {
                // A partial fill must not erase an outstanding asynchronous
                // lifecycle request.
                //
                // Example:
                //
                // Working
                //     -> PendingCancel
                //     -> partial Fill
                //     -> STILL PendingCancel
                //     -> OrderCanceled
                //
                // Likewise:
                //
                // Working
                //     -> PendingReplace
                //     -> partial Fill
                //     -> STILL PendingReplace
                //     -> OrderReplaced
                //
                // For ordinary active orders, a partial fill leaves the order
                // Working.
                order.state =
                    match previous_order_state {
                        OrderState::PendingCancel => {
                            OrderState::PendingCancel
                        }

                        OrderState::PendingReplace => {
                            OrderState::PendingReplace
                        }

                        _ => {
                            OrderState::Working
                        }
                    };
            }
        }


        // ============================================================
        // REDUCE RISK RESERVATION
        // ============================================================

        // Every execution consumes part of the quantity previously reserved
        // for this Order.
        //
        // When the reservation reaches zero, RiskReservations removes it.
        self.state
            .risk
            .reservations
            .reduce_quantity(
                fill.order_id,
                fill.quantity,
                fill.ts_ms,
            );


        // ============================================================
        // UPDATE POSITION
        // ============================================================

        // WARNING:
        // The engine currently assumes one execution account per venue.
        //
        // `"primary"` is a placeholder account identity used only to preserve
        // the PositionKey(venue, account, symbol) shape.
        //
        // If multi-account execution is ever added, replace this with a
        // canonical AccountId carried through Order -> Fill -> PositionKey.
        let position_key =
            PositionKey::new(
                fill.venue,
                "primary",
                fill.symbol.clone(),
            );


        let position =
            self.state.positions
                .get_or_create(
                    position_key,
                    fill.ts_ms,
                );


        apply_fill_to_position(
            position,
            &fill,
        );


        // ============================================================
        // UPDATE TRADE
        // ============================================================

        if let Some(trade_id) =
            trade_id
        {
            let Some(trade) =
                self.state.trades.get_mut(trade_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] TRADE DISAPPEARED BEFORE FILL APPLY | order_id={} | trade_id={}",
                        fill.order_id,
                        trade_id,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. The Trade was validated immediately before
                // this point, so disappearance indicates state inconsistency.

                return;
            };


            match order_purpose {
                // ====================================================
                // ENTRY
                // ====================================================

                OrderPurpose::Entry => {
                    let old_quantity =
                        trade.quantity;


                    // Calculate entry VWAP from all confirmed entry fills.
                    trade.avg_entry_px =
                        Some(
                            weighted_average(
                                trade.avg_entry_px,
                                old_quantity,
                                fill.price,
                                fill.quantity,
                            )
                        );


                    // Trade.quantity represents currently established OPEN
                    // quantity, so confirmed entry fills increase it.
                    trade.quantity =
                        old_quantity
                            + fill.quantity;


                    // The first confirmed entry execution establishes the
                    // canonical opening timestamp.
                    //
                    // Event time is used rather than Instant/SystemTime so
                    // timing semantics remain identical in live trading,
                    // simulation, and historical backtesting.
                    if trade.opened_ts_ms.is_none() {
                        trade.opened_ts_ms =
                            Some(fill.ts_ms);
                    }


                    // The Trade becomes fully Open when the complete requested
                    // entry Order has executed.
                    //
                    // If the entry only partially fills and is later canceled
                    // or expires, order_canceled.rs / order_expired.rs will
                    // promote the partially-established Trade to Open.
                    if order_fully_filled {
                        trade.state =
                            TradeState::Open;
                    }
                }


                // ====================================================
                // EXIT-LIKE ORDERS
                // ====================================================

                OrderPurpose::Exit
                | OrderPurpose::StopLoss
                | OrderPurpose::TakeProfit
                | OrderPurpose::EmergencyExit => {
                    let old_exited_quantity =
                        trade.exited_quantity;


                    // Calculate true exit VWAP across partial fills and
                    // multiple exit executions.
                    trade.avg_exit_px =
                        Some(
                            weighted_average(
                                trade.avg_exit_px,
                                old_exited_quantity,
                                fill.price,
                                fill.quantity,
                            )
                        );


                    // exited_quantity is cumulative historical exit quantity.
                    trade.exited_quantity +=
                        fill.quantity;


                    // quantity represents CURRENTLY OPEN trade quantity.
                    trade.quantity =
                        (trade.quantity - fill.quantity)
                            .max(0.0);


                    if trade.quantity <= f64::EPSILON {
                        trade.quantity = 0.0;

                        trade.state =
                            TradeState::Closed;

                        trade.closed_ts_ms =
                            Some(fill.ts_ms);
                    } else {
                        // Some exposure still remains after this fill.
                        trade.state =
                            TradeState::Closing;
                    }
                }


                // ====================================================
                // NON-DIRECTIONAL ORDER PURPOSES
                // ====================================================

                OrderPurpose::Quote
                | OrderPurpose::Hedge => {
                    // Quote/Hedge Orders can affect Position state without
                    // necessarily belonging to a directional Trade lifecycle.
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
                "[ENGINE] FILL APPLIED | execution_id={:?} | order_id={} | symbol={} | side={:?} | qty={} | price={} | order_filled={}",
                fill.execution_id,
                fill.order_id,
                fill.symbol,
                fill.side,
                fill.quantity,
                fill.price,
                order_fully_filled,
            ),
        );
    }
}


// ============================================================
// POSITION ACCOUNTING
// ============================================================

/// Applies one confirmed buy/sell execution to an actual venue Position.
///
/// Handles:
///
/// - opening exposure from flat
/// - increasing existing exposure
/// - partially reducing exposure
/// - fully closing exposure
/// - crossing through flat into the opposite direction
///
/// Position quantity is signed:
///
/// positive = long
/// negative = short
/// zero     = flat
fn apply_fill_to_position(
    position: &mut crate::core::positions::Position,
    fill: &Fill,
) {
    let signed_fill_quantity =
        match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };


    let old_quantity =
        position.quantity;

    let new_quantity =
        old_quantity
            + signed_fill_quantity;


    // ============================================================
    // OPEN FROM FLAT
    // ============================================================

    if old_quantity.abs()
        <= f64::EPSILON
    {
        position.quantity =
            new_quantity;

        position.avg_entry_px =
            Some(fill.price);

        position.updated_ts_ms =
            fill.ts_ms;

        return;
    }


    // ============================================================
    // INCREASE SAME DIRECTION
    // ============================================================

    if old_quantity.signum()
        == signed_fill_quantity.signum()
    {
        position.avg_entry_px =
            Some(
                weighted_average(
                    position.avg_entry_px,
                    old_quantity.abs(),
                    fill.price,
                    fill.quantity,
                )
            );

        position.quantity =
            new_quantity;

        position.updated_ts_ms =
            fill.ts_ms;

        return;
    }


    // ============================================================
    // REDUCE / CLOSE / FLIP
    // ============================================================

    let closing_quantity =
        old_quantity
            .abs()
            .min(fill.quantity);


    if let Some(avg_entry_px) =
        position.avg_entry_px
    {
        let realized =
            if old_quantity > 0.0 {
                (fill.price - avg_entry_px)
                    * closing_quantity
            } else {
                (avg_entry_px - fill.price)
                    * closing_quantity
            };

        position.realized_pnl +=
            realized;
    }


    position.quantity =
        new_quantity;


    if new_quantity.abs()
        <= f64::EPSILON
    {
        // Position is completely flat.
        position.quantity = 0.0;
        position.avg_entry_px = None;
    } else if new_quantity.signum()
        != old_quantity.signum()
    {
        // The Fill crossed through flat and established exposure in the
        // opposite direction. The excess quantity starts a new position at
        // this execution price.
        position.avg_entry_px =
            Some(fill.price);
    }


    position.updated_ts_ms =
        fill.ts_ms;
}


// ============================================================
// WEIGHTED AVERAGE EXECUTION PRICE
// ============================================================

// ENTRY FILLS
//     ↓
// quantity ↑
// avg_entry_px = entry VWAP
//
// EXIT FILLS
//     ↓
// quantity ↓
// exited_quantity ↑
// avg_exit_px = exit VWAP
//
// quantity reaches 0
//     ↓
// TradeState::Closed

/// Calculates the quantity-weighted average execution price after applying
/// a new Fill to an existing set of executions.
///
/// This is used when an Order or Trade receives multiple fills at different
/// prices. The existing average is weighted by the quantity already executed,
/// while the new fill price is weighted by the new fill quantity.
///
/// Example:
///
///     existing: 0.25 BTC @ $80,000
///     new fill:  0.50 BTC @ $81,000
///
/// The resulting average represents the true average execution price across
/// the full 0.75 BTC rather than simply using the newest or oldest fill price.
///
/// `current_quantity` must represent the quantity that contributed to
/// `current_avg`.
///
/// For entry VWAP:
///
///     current_quantity = previously entered quantity
///
/// For exit VWAP:
///
///     current_quantity = previously exited quantity
fn weighted_average(
    current_avg: Option<f64>,
    current_quantity: f64,
    fill_price: f64,
    fill_quantity: f64,
) -> f64 {
    let Some(current_avg) =
        current_avg
    else {
        return fill_price;
    };


    let total_quantity =
        current_quantity
            + fill_quantity;


    if total_quantity <= 0.0 {
        return fill_price;
    }


    (
        current_avg
            * current_quantity
        +
        fill_price
            * fill_quantity
    )
        / total_quantity
}