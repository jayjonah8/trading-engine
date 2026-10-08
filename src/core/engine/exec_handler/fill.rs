#![allow(dead_code)]

use crate::{
    core::{
        engine::{
            engine::Engine,
            state::ExecutionMode,
        },

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
    ///     -> validate Fill price and quantity
    ///     -> validate against Order and remaining quantity
    ///     -> validate associated Trade and lifecycle state
    ///     -> validate exit quantity and confirmed entry VWAP
    ///     -> deduplicate through FillLedger
    ///     -> update Order execution state and VWAP
    ///     -> reduce RiskReservation
    ///     -> update Position
    ///     -> update Trade quantity, VWAP, and lifecycle
    ///     -> calculate realized PnL for exit fills
    ///     -> update General accounting and closed-trade counters
    ///
    /// A Fill is an authoritative execution fact that changes actual exposure.
    ///
    /// Submitting, accepting, working, canceling, or replacing an Order does
    /// not itself change Position or Trade quantity. Confirmed fills do.
    ///
    /// Realized PnL is calculated on each confirmed exit fill, supporting
    /// long/short positions, partial exits, and multiple exit executions.
    ///
    /// Accounting is separated by execution mode:
    ///
    /// - Live/Paper/Backtest: realized_pnl, live_closed_trades
    /// - Simulation: sim_realized_pnl, sim_closed_trades
    ///
    /// Closed-trade counters increment only when a logical Trade is fully
    /// closed. Realized PnL accumulates across all confirmed exit fills.
    ///
    /// Trade timing uses canonical event timestamps:
    ///
    ///     trade.opened_ts_ms = first confirmed entry fill timestamp
    ///     trade.closed_ts_ms = final confirmed exit fill timestamp
    ///
    /// Risk logic compares market-event `ts_ms` against `opened_ts_ms`.
    /// This preserves deterministic timing across live trading, simulation,
    /// and historical backtesting.
    ///
    /// 
    /// TODO:
    /// Incorporate venue execution fees and funding payments into net PnL.
    ///
    /// TODO:
    /// Implement live execution reconciliation for unexpected confirmed
    /// fills. A venue execution cannot be undone simply because validation
    /// fails. Preserve the execution fact, reconcile actual venue exposure,
    /// correct inconsistent engine state, and notify via Slack/Discord.
    /// 
    /// In live trading, unexpected executions must eventually trigger
    /// reconciliation to:
    /// 
    /// - Preserve the confirmed execution fact.
    /// - Query actual venue Orders and Positions.
    /// - Correct inconsistent engine exposure and accounting.
    /// - Notify the operator through Slack/Discord.
    ///
    /// Until reconciliation is implemented, rejected live Fills
    /// may leave engine state inconsistent with actual venue exposure.
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

        // Validate the logical Trade BEFORE recording the execution
        // or modifying Order, Position, RiskReservation, or PnL.
        //
        // An Order can have sufficient remaining quantity while its
        // associated Trade has already been partially or fully closed.
        //
        // These checks protect canonical accounting from inconsistent
        // execution events.
        //
        // IMPORTANT:
        // A confirmed live venue execution cannot simply be undone.
        // Rejected fills indicating actual venue exposure must eventually
        // trigger reconciliation rather than being silently discarded.

        if let Some(trade_id) = trade_id {
            let Some(trade) =
                self.state.trades.get(trade_id)
            else {
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
                // Send a critical Slack/Discord notification in live trading.
                // A confirmed execution referencing a missing Trade indicates
                // inconsistent engine state and requires reconciliation.

                return;
            };


            match order_purpose {
                // ====================================================
                // ENTRY FILL VALIDATION
                // ====================================================

                OrderPurpose::Entry => {
                    // A directional Trade must not accept new entry fills
                    // after it has completely closed.
                    //
                    // Otherwise, a delayed execution could reopen a
                    // previously completed logical Trade and corrupt
                    // its quantity, entry VWAP, and PnL accounting.
                    if trade.state == TradeState::Closed {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] ENTRY FILL FOR CLOSED TRADE | trade_id={} | order_id={} | symbol={}",
                                trade.id,
                                fill.order_id,
                                fill.symbol,
                            ),
                        );

                        // TODO:
                        // Send a critical Slack/Discord notification.
                        //
                        // Reconcile the actual venue position because
                        // this entry execution may have established
                        // new exposure after the logical Trade closed.

                        return;
                    }


                    // TODO (POTENTIAL EXTRA CHECK):
                    //
                    // Detect entry fills arriving after a Trade has
                    // started exiting (TradeState::Closing).
                    //
                    // A partially filled entry Order may continue
                    // receiving fills while cancellation is pending.
                    //
                    // Simply rejecting these confirmed executions is
                    // insufficient in live trading because the venue
                    // may have already increased actual exposure.
                    //
                    // Future handling should:
                    //
                    // 1. Detect outstanding entry Orders when exiting.
                    // 2. Request cancellation when appropriate.
                    // 3. Reconcile late entry fills against actual exposure.
                    // 4. Maintain correct cumulative entry cost basis.
                    //
                    // TODO:
                    // Send a critical Slack/Discord notification if
                    // unexpected late entry executions are detected.
                }


                // ====================================================
                // EXIT FILL VALIDATION
                // ====================================================

                OrderPurpose::Exit
                | OrderPurpose::StopLoss
                | OrderPurpose::TakeProfit
                | OrderPurpose::EmergencyExit => {
                    // ====================================================
                    // VALIDATE TRADE LIFECYCLE
                    // ====================================================

                    // A completed Trade must never be closed again.
                    //
                    // FillLedger prevents duplicate execution IDs,
                    // but different execution IDs could still reference
                    // multiple exit Orders for the same Trade.
                    if trade.state == TradeState::Closed {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] EXIT FILL FOR CLOSED TRADE | trade_id={} | order_id={} | symbol={}",
                                trade.id,
                                fill.order_id,
                                fill.symbol,
                            ),
                        );

                        // TODO:
                        // Send a critical Slack/Discord notification.
                        //
                        // Reconcile the actual venue position because
                        // an additional exit execution may have caused
                        // unintended exposure or a position reversal.

                        return;
                    }


                    // ====================================================
                    // VALIDATE REMAINING TRADE QUANTITY
                    // ====================================================

                    // Order.remaining_quantity() was validated earlier.
                    //
                    // Also validate against the actual remaining Trade
                    // quantity because multiple exit Orders could
                    // individually pass the Order-level overfill guard.
                    //
                    // This prevents:
                    //
                    // - exiting more quantity than the Trade owns
                    // - incorrect cumulative exited quantity
                    // - incorrect realized PnL
                    // - accidental duplicate Trade closures
                    if !trade.quantity.is_finite()
                        || trade.quantity <= 0.0
                        || fill.quantity > trade.quantity + f64::EPSILON
                    {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] TRADE EXIT OVERFILL REJECTED | trade_id={} | order_id={} | symbol={} | fill_qty={} | trade_remaining_qty={}",
                                trade.id,
                                fill.order_id,
                                fill.symbol,
                                fill.quantity,
                                trade.quantity,
                            ),
                        );

                        // TODO:
                        // Send a critical Slack/Discord notification.
                        //
                        // A confirmed venue execution exceeding the
                        // remaining logical Trade quantity requires
                        // reconciliation against actual venue exposure.

                        return;
                    }


                    // ====================================================
                    // VALIDATE CONFIRMED ENTRY PRICE
                    // ====================================================

                    // Realized PnL requires the confirmed entry VWAP.
                    //
                    // Reject missing, non-finite, or non-positive
                    // entry prices before modifying canonical state.
                    let valid_entry_px =
                        trade.avg_entry_px
                            .map(|px| px.is_finite() && px > 0.0)
                            .unwrap_or(false);

                    if !valid_entry_px {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] EXIT FILL INVALID ENTRY PRICE | trade_id={} | order_id={} | symbol={} | avg_entry_px={:?}",
                                trade.id,
                                fill.order_id,
                                fill.symbol,
                                trade.avg_entry_px,
                            ),
                        );

                        // TODO:
                        // Send a critical Slack/Discord notification.
                        //
                        // Missing or invalid entry VWAP prevents
                        // reliable realized PnL calculation and
                        // indicates inconsistent Trade accounting.

                        return;
                    }


                    // ====================================================
                    // VALIDATE EXIT STATE
                    // ====================================================

                    // Exit executions are expected only for Trades
                    // that have established exposure.
                    //
                    // Open:
                    //     An exit may have been submitted and filled
                    //     before the Trade transitioned to Closing.
                    //
                    // Closing:
                    //     An exit is already in progress.
                    //
                    // Other states should not receive exit fills.
                    if trade.state != TradeState::Open
                        && trade.state != TradeState::Closing
                    {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] EXIT FILL INVALID TRADE STATE | trade_id={} | order_id={} | symbol={} | state={:?}",
                                trade.id,
                                fill.order_id,
                                fill.symbol,
                                trade.state,
                            ),
                        );

                        // TODO:
                        // Send a critical Slack/Discord notification.
                        //
                        // Investigate unexpected Trade lifecycle
                        // transitions and reconcile venue exposure.

                        return;
                    }
                }


                // ====================================================
                // NON-DIRECTIONAL ORDERS
                // ====================================================

                OrderPurpose::Quote
                | OrderPurpose::Hedge => {
                    // Quote/Hedge executions may not follow the
                    // directional Trade lifecycle.
                    //
                    // Position accounting remains authoritative.
                }
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
                    // ====================================================
                    // CALCULATE REALIZED PNL
                    // ====================================================

                    // Every confirmed exit fill realizes PnL immediately.
                    //
                    // LONG:
                    //     (exit_price - entry_price) * exit_quantity
                    //
                    // SHORT:
                    //     (entry_price - exit_price) * exit_quantity
                    //
                    // Calculating PnL per fill correctly handles partial exits
                    // and multiple exit executions at different prices.
                    //
                    // TODO:
                    // Incorporate venue execution fees and funding payments
                    // into net realized PnL.


                    // Entry VWAP was validated before applying the Fill.
                    //
                    // This additional check prevents a missing entry price
                    // from causing a panic in the authoritative engine.
                    let Some(entry_px) =
                        trade.avg_entry_px
                    else {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] CRITICAL MISSING ENTRY PRICE | trade_id={} | order_id={}",
                                trade.id,
                                fill.order_id,
                            ),
                        );

                        // TODO:
                        // Send a critical Slack/Discord notification.
                        // Reconcile authoritative engine state before continuing.
                        //
                        // WARNING:
                        // This condition occurs after earlier state mutations.
                        // The engine must not treat this as a safely rejected Fill.

                        return;
                    };


                    let realized_pnl =
                        match fill.side {
                            // Selling closes a long Trade.
                            Side::Sell => {
                                (fill.price - entry_px)
                                    * fill.quantity
                            }

                            // Buying closes a short Trade.
                            Side::Buy => {
                                (entry_px - fill.price)
                                    * fill.quantity
                            }
                        };


                    // ====================================================
                    // UPDATE GENERAL REALIZED PNL
                    // ====================================================

                    // Live, Paper, and Backtest share the primary PnL fields.
                    //
                    // Simulation maintains separate simulated accounting.
                    //
                    // Both paths use the same canonical Fill lifecycle.
                    match self.execution_mode {
                        ExecutionMode::Live
                        | ExecutionMode::Paper
                        | ExecutionMode::Backtest => {
                            self.general.realized_pnl += realized_pnl;
                        }

                        ExecutionMode::Simulation => {
                            self.general.sim_realized_pnl += realized_pnl;
                        }
                    }


                    // ====================================================
                    // UPDATE TRADE EXIT VWAP
                    // ====================================================

                    let old_exited_quantity =
                        trade.exited_quantity;


                    // Calculate the cumulative exit VWAP across all
                    // confirmed exit fills for this logical Trade.
                    trade.avg_exit_px =
                        Some(
                            weighted_average(
                                trade.avg_exit_px,
                                old_exited_quantity,
                                fill.price,
                                fill.quantity,
                            )
                        );


                    // Total quantity exited throughout the Trade lifecycle.
                    trade.exited_quantity +=
                        fill.quantity;


                    // Remaining currently open Trade quantity.
                    trade.quantity =
                        (trade.quantity - fill.quantity)
                            .max(0.0);


                    // ====================================================
                    // HANDLE FULL TRADE CLOSURE
                    // ====================================================

                    if trade.quantity <= f64::EPSILON {
                        trade.quantity = 0.0;

                        trade.state =
                            TradeState::Closed;

                        trade.closed_ts_ms =
                            Some(fill.ts_ms);


                        // ====================================================
                        // UPDATE CLOSED TRADE COUNTERS
                        // ====================================================

                        // Count each completed logical Trade once.
                        //
                        // Partial exit fills realize PnL immediately but
                        // do not increment the closed-trade counter.
                        //
                        // Live/Paper/Backtest share live_closed_trades.
                        // Simulation uses sim_closed_trades.
                        match self.execution_mode {
                            ExecutionMode::Live
                            | ExecutionMode::Paper
                            | ExecutionMode::Backtest => {
                                self.general.live_closed_trades += 1;
                            }

                            ExecutionMode::Simulation => {
                                self.general.sim_closed_trades += 1;
                            }
                        }


                        // ====================================================
                        // CALCULATE TOTAL TRADE PNL
                        // ====================================================

                        // Entry and exit VWAPs represent the confirmed
                        // executions associated with this logical Trade.
                        //
                        // This is gross realized PnL before fees and funding.
                        let avg_exit_px =
                            trade.avg_exit_px.unwrap_or(fill.price);

                        let total_trade_pnl =
                            match fill.side {
                                Side::Sell => {
                                    (avg_exit_px - entry_px)
                                        * trade.exited_quantity
                                }

                                Side::Buy => {
                                    (entry_px - avg_exit_px)
                                        * trade.exited_quantity
                                }
                            };


                        // ====================================================
                        // LOG CLOSED TRADE
                        // ====================================================

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] TRADE CLOSED | trade_id={} | symbol={} | exited_qty={} | entry_px={:.6} | exit_px={:.6} | pnl=${:.4} | ts_ms={}",
                                trade.id,
                                trade.symbol,
                                trade.exited_quantity,
                                entry_px,
                                avg_exit_px,
                                total_trade_pnl,
                                fill.ts_ms,
                            ),
                        );
                    } else {
                        // Some exposure remains after this confirmed exit.
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