#![allow(dead_code)]

use std::time::{
    Instant,
    SystemTime,
};

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
    ///     -> deduplicate through FillLedger
    ///     -> update Order execution state
    ///     -> reduce RiskReservation
    ///     -> update Position
    ///     -> update associated Trade
    ///
    /// A fill is the execution fact that changes actual exposure.
    /// Submitting or accepting an order does not change Position state.
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
            self.state.orders.get(
                fill.order_id
            )
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

            return;
        }

        let remaining_quantity =
            order.remaining_quantity();

        if fill.quantity > remaining_quantity {
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

            return;
        }

        let trade_id =
            order.trade_id;

        let order_purpose =
            order.purpose;


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

                return;
            }
        }


        // ============================================================
        // DEDUPLICATE FILL
        // ============================================================

        // Record the execution before applying any state mutation.
        //
        // If the same execution is received again from an immediate response,
        // WebSocket update, reconnect, polling, or reconciliation, it must not
        // be applied twice.
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
            let order =
                self.state.orders
                    .get_mut(fill.order_id)
                    .expect(
                        "validated order disappeared before fill application"
                    );

            let old_filled_quantity =
                order.filled_quantity;

            let new_filled_quantity =
                old_filled_quantity
                    + fill.quantity;

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
                order.state =
                    OrderState::Filled;
            } else {
                order.state =
                    OrderState::Working;
            }
        }

        // ============================================================
        // REDUCE RISK RESERVATION
        // ============================================================

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
        // `"primary"` is therefore a placeholder account identity used only to
        // preserve the PositionKey(venue, account, symbol) shape.
        //
        // If multi-account execution is added later, replace this with a canonical
        // AccountId carried through Order -> Fill -> PositionKey.
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
            let trade =
                self.state.trades
                    .get_mut(trade_id)
                    .expect(
                        "validated trade disappeared before fill application"
                    );

            match order_purpose {
                // ====================================================
                // ENTRY
                // ====================================================

                OrderPurpose::Entry => {
                    let old_quantity =
                        trade.quantity;

                    trade.avg_entry_px =
                        Some(
                            weighted_average(
                                trade.avg_entry_px,
                                old_quantity,
                                fill.price,
                                fill.quantity,
                            )
                        );

                    trade.quantity =
                        old_quantity
                            + fill.quantity;

                    if trade.opened_ts_ms.is_none() {
                        trade.opened_ts_ms =
                            Some(fill.ts_ms);
                    }

                    if order_fully_filled {
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

                    trade.avg_exit_px =
                        Some(
                            weighted_average(
                                trade.avg_exit_px,
                                old_exited_quantity,
                                fill.price,
                                fill.quantity,
                            )
                        );

                    trade.exited_quantity +=
                        fill.quantity;

                    trade.quantity =
                        (trade.quantity - fill.quantity)
                            .max(0.0);

                    if trade.quantity <= f64::EPSILON {
                        trade.quantity = 0.0;
                        trade.state = TradeState::Closed;
                        trade.closed_ts_ms = Some(fill.ts_ms);
                    } else {
                        trade.state = TradeState::Closing;
                    }
                }

                // ====================================================
                // NON-DIRECTIONAL ORDER PURPOSES
                // ====================================================

                OrderPurpose::Quote
                | OrderPurpose::Hedge => {
                    // Quote/Hedge orders can affect Position state without
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

/// Applies one confirmed buy/sell execution to an actual venue position.
///
/// Handles:
///
/// - opening a new position
/// - increasing an existing position
/// - partially reducing exposure
/// - fully closing exposure
/// - crossing through flat into the opposite direction
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

    if old_quantity == 0.0 {
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
        position.quantity = 0.0;
        position.avg_entry_px = None;
    } else if new_quantity.signum()
        != old_quantity.signum()
    {
        // The execution crossed through flat and established exposure
        // in the opposite direction.
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

// EXIT FILLS
//     ↓
// quantity ↓
// exited_quantity ↑
// avg_exit_px = exit VWAP

// quantity reaches 0
//     ↓
// TradeState::Closed

/// Calculates the quantity-weighted average execution price after applying
/// a new fill to an existing set of executions.
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
/// `current_avg`. For entry VWAP this is previously entered quantity; for
/// exit VWAP this is previously exited quantity.
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