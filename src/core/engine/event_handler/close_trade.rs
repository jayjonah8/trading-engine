#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,

        execution::{
            ClientOrderId,
            ExecCommand,
            Order,
            OrderPurpose,
            OrderState,
            OrderType,
            Side,
            TimeInForce,
        },

        risk::{
            ReservationPurpose,
            RiskReservation,
        },

        trades::{
            Direction,
            TradeId,
            TradeState,
        },
    },

    shared::print_log,
};


// ============================================================
// CLOSE TRADE REQUEST FLOW
// ============================================================

// EngineEvent::CloseTrade
//     = "The engine wants to close this specific logical Trade."
//
// CloseTrade
//     ↓
// find Trade by TradeId
//     ↓
// require TradeState::Open
//     ↓
// determine remaining open quantity
//     ↓
// ensure no exit order/reservation is already active
//     ↓
// determine exit side
//
// Long  -> Sell
// Short -> Buy
//
//     ↓
// create canonical Exit Order
//
// purpose       = Exit
// order_type    = Market
// time_in_force = IOC
// reduce_only   = true
//
//     ↓
// create Exit RiskReservation
//     ↓
// TradeState::Open -> Closing
//     ↓
// insert Order + reservation
//     ↓
// ExecCommand::SubmitOrder
//     ↓
// execution router
//     ↓
// venue adapter
//     ↓
// ExecEvent::Fill
//     ↓
// fill.rs reduces Trade / Position
//     ↓
// remaining Trade.quantity reaches zero
//     ↓
// TradeState::Closed
//
// IMPORTANT:
//
// `close_trade.rs` does NOT directly close the Trade.
//
// It only creates the execution request and transitions the logical Trade to
// `Closing`. Confirmed fills remain the only authority for reducing actual
// exposure and eventually transitioning the Trade to `Closed`.


// ============================================================
// MARKET EXIT POLICY
// ============================================================

// The first close implementation uses a market-style exit because CloseTrade
// means "reduce this exposure now".
//
// IOC means:
//
// - execute as much quantity as possible immediately
// - cancel any remainder that cannot execute immediately
//
// FOK is intentionally not used because an inability to fill the entire
// quantity immediately should not prevent a useful partial reduction.
//
// GTC / Day are more appropriate for working/resting orders rather than this
// immediate market-exit intent.
//
// Venue adapters remain responsible for translating this canonical Market +
// IOC intent into the closest supported behavior at their venue.


// ============================================================
// HANDLE CLOSE TRADE
// ============================================================

impl Engine {
    /// Requests closure of one specific logical strategy Trade.
    ///
    /// This is the REQUEST side of the exit lifecycle.
    ///
    /// The handler:
    ///
    /// - finds the Trade by canonical TradeId
    /// - verifies that it is currently Open
    /// - prevents duplicate simultaneous exit attempts
    /// - creates a reduce-only market Exit Order
    /// - creates an Exit RiskReservation
    /// - transitions the Trade to `Closing`
    /// - submits the Order through the execution router
    /// - rolls back newly-created request state if router delivery fails
    ///
    /// Confirmed fills are responsible for changing actual Position exposure
    /// and reducing the Trade's remaining quantity.
    pub fn handle_close_trade(
        &mut self,
        trade_id: TradeId,
        reason: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // FIND TRADE
        // ============================================================

        let Some(trade) =
            self.state.trades.get(trade_id)
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE REJECTED | trade_id={} | reason=unknown trade | request_reason={}",
                    trade_id,
                    reason,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading
            // when a close request references an unknown TradeId.

            return;
        };


        // ============================================================
        // TRADE STATE
        // ============================================================

        // The first implementation only closes fully established Open trades.
        //
        // Opening trades should instead have their working entry Order
        // canceled. Closing trades already have an exit lifecycle underway.
        // Closed trades have no remaining logical exposure.
        if trade.state != TradeState::Open {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE REJECTED | trade_id={} | symbol={} | state={:?} | reason=trade is not Open | request_reason={}",
                    trade.id,
                    trade.symbol,
                    trade.state,
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // REMAINING QUANTITY
        // ============================================================

        if !trade.quantity.is_finite()
            || trade.quantity <= f64::EPSILON
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE REJECTED | trade_id={} | symbol={} | quantity={} | reason=no valid remaining quantity",
                    trade.id,
                    trade.symbol,
                    trade.quantity,
                ),
            );

            return;
        }


        // ============================================================
        // DUPLICATE EXIT RESERVATION GUARD
        // ============================================================

        // Exit reservations prevent multiple close requests from reserving
        // and attempting to close the same Trade quantity simultaneously.
        let reserved_exit_quantity =
            self.state
                .risk
                .reservations
                .exit_quantity_for_trade(trade_id);

        if reserved_exit_quantity > f64::EPSILON {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE REJECTED | trade_id={} | symbol={} | reserved_exit_qty={} | reason=exit already reserved",
                    trade.id,
                    trade.symbol,
                    reserved_exit_quantity,
                ),
            );

            return;
        }


        // ============================================================
        // ACTIVE EXIT ORDER GUARD
        // ============================================================

        // Keep a second defensive guard against an active exit-like Order
        // existing without the expected reservation.
        let has_active_exit_order =
            self.state
                .orders
                .active_for_trade(trade_id)
                .any(|order| {
                    matches!(
                        order.purpose,
                        OrderPurpose::Exit
                            | OrderPurpose::StopLoss
                            | OrderPurpose::TakeProfit
                            | OrderPurpose::EmergencyExit
                    )
                });

        if has_active_exit_order {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE REJECTED | trade_id={} | symbol={} | reason=active exit order already exists",
                    trade.id,
                    trade.symbol,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord error notification in live trading
            // if an active exit Order exists without the expected reservation,
            // because that may indicate inconsistent risk state.

            return;
        }


        // ============================================================
        // CAPTURE TRADE METADATA
        // ============================================================

        // Capture everything needed before mutating Trades.
        let strategy_id =
            trade.strategy_id.clone();

        let symbol =
            trade.symbol.clone();

        let direction =
            trade.direction;

        let quantity =
            trade.quantity;


        // ============================================================
        // EXIT SIDE
        // ============================================================

        // Direction describes the logical Trade.
        // Side describes what the execution venue must do.
        //
        // Long exposure is closed by selling.
        // Short exposure is closed by buying.
        let side =
            match direction {
                Direction::Long => Side::Sell,
                Direction::Short => Side::Buy,
            };


        // ============================================================
        // EXECUTION VENUE
        // ============================================================

        // The current engine configuration intentionally uses one execution
        // venue for the runtime.
        //
        // The canonical Order carries the venue and the execution router uses
        // that field to select the correct venue adapter.
        let venue =
            self.cfg.execution_venue;


        // ============================================================
        // ALLOCATE ORDER ID
        // ============================================================

        let order_id =
            self.state.allocate_order_id();


        // ============================================================
        // CLIENT ORDER ID
        // ============================================================

        // Generate a globally unique client-order identity.
        //
        // This is separate from:
        //
        // - OrderId           -> internal engine identity
        // - venue_order_id    -> venue-assigned order identity
        // - venue_execution_id -> individual fill/execution identity
        //
        // Execution adapters may translate this value into their venue-specific
        // client-order identifier format.
        //
        // Examples:
        //
        // Hyperliquid -> CLOID
        // Alpaca      -> client_order_id
        //
        // The same ClientOrderId remains attached to this canonical Order for its
        // entire lifecycle.
        let client_order_id =
            ClientOrderId::new();


        // ============================================================
        // EXIT ORDER
        // ============================================================

        let order =
            Order {
                id: order_id,
                venue,
                venue_order_id: None,
                client_order_id,
                strategy_id: strategy_id.clone(),
                trade_id: Some(trade_id),
                symbol: symbol.clone(),
                side,
                purpose: OrderPurpose::Exit,

                // Immediate close request.
                order_type: OrderType::Market,
                time_in_force: TimeInForce::Ioc,

                quantity,

                // Market exit has no canonical limit/stop price.
                limit_px: None,
                stop_px: None,

                // The exit must reduce existing exposure rather than
                // intentionally establish exposure in the opposite direction.
                reduce_only: true,

                // A close-market request should take available liquidity.
                post_only: false,

                filled_quantity: 0.0,
                avg_fill_px: None,

                pending_replace: None,

                state: OrderState::PendingSubmit,

                created_ts_ms: ts_ms,
                updated_ts_ms: ts_ms,
            };


        // ============================================================
        // EXIT RESERVATION NOTIONAL
        // ============================================================

        // Exit reservations primarily protect quantity from being closed by
        // multiple simultaneous exit orders.
        //
        // Keep an approximate notional as useful canonical risk metadata.
        // Prefer current market price, then average entry price, then the
        // original entry signal price as a fallback.
        let reference_px =
            self.last_px
                .get(&symbol)
                .map(|last| last.px)
                .or_else(|| {
                    self.state.trades
                        .get(trade_id)
                        .and_then(|trade| {
                            trade.avg_entry_px
                        })
                })
                .or_else(|| {
                    self.state.trades
                        .get(trade_id)
                        .map(|trade| {
                            trade.entry_signal_px
                        })
                })
                .unwrap_or(0.0);

        let reserved_notional =
            quantity
                * reference_px;


        // ============================================================
        // EXIT RISK RESERVATION
        // ============================================================

        let reservation =
            RiskReservation {
                order_id,
                trade_id: Some(trade_id),
                strategy_id: strategy_id.clone(),
                symbol: symbol.clone(),
                purpose: ReservationPurpose::Exit,
                quantity,
                notional: reserved_notional,
                created_ts_ms: ts_ms,
                updated_ts_ms: ts_ms,
            };


        // ============================================================
        // TRANSITION TRADE TO CLOSING
        // ============================================================

        // Closing means an exit is being worked while confirmed exposure may
        // still remain.
        //
        // Do NOT set Closed here. Only confirmed Fill events can reduce the
        // remaining Trade quantity to zero.
        {
            let Some(trade) =
                self.state.trades.get_mut(trade_id)
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE] CLOSE TRADE STATE LOST BEFORE COMMIT | trade_id={} | symbol={}",
                        trade_id,
                        symbol,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. The Trade was validated immediately before
                // this point, so disappearance indicates state inconsistency.

                return;
            };

            trade.state =
                TradeState::Closing;
        }


        // ============================================================
        // COMMIT ORDER + RESERVATION
        // ============================================================

        self.state.orders.insert(
            order.clone()
        );

        self.state
            .risk
            .reservations
            .insert(
                reservation
            );


        // ============================================================
        // SUBMIT EXIT ORDER
        // ============================================================

        // A successful channel send only means the execution router received
        // the command. The Order remains PendingSubmit until execution events
        // report what actually happened at the venue.
        if let Err(err) =
            self.tx_exec.send(
                ExecCommand::SubmitOrder {
                    order,
                }
            )
        {
            // ========================================================
            // ROLLBACK REQUEST STATE
            // ========================================================

            // The command never reached the execution router, so remove the
            // newly-created Order/reservation and restore the Trade to Open.
            self.state
                .risk
                .reservations
                .release(order_id);

            self.state
                .orders
                .remove(order_id);


            if let Some(trade) =
                self.state.trades.get_mut(trade_id)
            {
                if trade.state == TradeState::Closing {
                    trade.state =
                        TradeState::Open;
                }
            }


            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE SUBMIT FAILED | trade_id={} | order_id={} | strategy={} | symbol={} | venue={:?} | reason={} | error={}",
                    trade_id,
                    order_id,
                    strategy_id,
                    symbol,
                    venue,
                    reason,
                    err,
                ),
            );

            // TODO:
            // Send a remote Slack/Discord critical error notification in live
            // trading when an exit command cannot reach the execution router.

            return;
        }


        // ============================================================
        // EXIT SUBMITTED
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] CLOSE TRADE SUBMITTED | trade_id={} | order_id={} | strategy={} | symbol={} | side={:?} | type=Market | tif=Ioc | qty={} | reduce_only=true | venue={:?} | reason={}",
                trade_id,
                order_id,
                strategy_id,
                symbol,
                side,
                quantity,
                venue,
                reason,
            ),
        );
    }
}