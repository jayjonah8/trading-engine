#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,

        execution::{
            ClientOrderId,
            ExecCommand,
            ExitOrderRequest,
            Order,
            OrderPurpose,
            OrderState,
            Side,
            TimeInForce,
            Venue,
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
// ExitOrderRequest
//     ↓
// validate exit execution instructions
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
// order_type    = requested execution type
// time_in_force = requested TIF
// limit_px      = requested limit price when applicable
// stop_px       = requested stop price when applicable
// post_only     = requested post-only policy
// reduce_only   = ALWAYS true
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
// OrderAccepted / OrderWorking / Fill / OrderCanceled / ...
//     ↓
// execution handlers update authoritative state
//
// IMPORTANT:
//
// `close_trade.rs` does NOT directly reduce actual exposure.
//
// It creates the canonical exit Order and transitions the logical Trade to
// `Closing`. Confirmed Fill events remain the only authority for reducing
// Position/Trade quantity and eventually transitioning the Trade to `Closed`.


// ============================================================
// EXIT EXECUTION POLICY
// ============================================================

// `ExitOrderRequest` describes HOW an existing Trade should be exited.
//
// Examples:
//
// Immediate market exit:
//
//     ExitOrderRequest::market()
//
//         -> Market
//         -> IOC
//         -> reduce_only = true
//
// Resting limit exit:
//
//     ExitOrderRequest::limit(
//         limit_px,
//         TimeInForce::Gtc,
//         post_only,
//     )
//
//         -> Limit
//         -> selected TIF
//         -> selected limit price
//         -> optional post-only
//         -> reduce_only = true
//
// The Engine, not the strategy/caller, owns `reduce_only`.
//
// A canonical directional Trade exit must never intentionally increase or
// reverse exposure. Therefore every Order created by this handler has:
//
//     reduce_only = true
//
// Venue adapters translate the resulting canonical Order into the closest
// supported venue-specific execution behavior.


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
    /// - validates the requested exit execution instructions
    /// - finds the Trade by canonical TradeId
    /// - verifies that it is currently Open
    /// - prevents duplicate simultaneous exit attempts
    /// - derives the exit side and quantity from authoritative Trade state
    /// - creates a canonical reduce-only Exit Order
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
        execution: ExitOrderRequest,
        reason: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // VALIDATE EXIT EXECUTION REQUEST
        // ============================================================

        // Reject malformed execution instructions before creating or mutating
        // any authoritative Trade, Order, Position, or Risk state.
        //
        // Examples of invalid requests:
        //
        // - Limit without limit_px
        // - Market with limit_px
        // - StopMarket without stop_px
        // - post_only on a non-limit order
        if let Err(error) =
            execution.validate()
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE REJECTED | trade_id={} | error={:?} | request_reason={}",
                    trade_id,
                    error,
                    reason,
                ),
            );

            return;
        }


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

        // Only fully established Open trades are eligible for a new exit
        // lifecycle.
        //
        // Opening trades should instead have their working entry Order
        // canceled.
        //
        // Closing trades already have an exit lifecycle underway.
        //
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
                .exit_quantity_for_trade(
                    trade_id
                );


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
                .active_for_trade(
                    trade_id
                )
                .any(
                    |order| {
                        matches!(
                            order.purpose,
                            OrderPurpose::Exit
                                | OrderPurpose::StopLoss
                                | OrderPurpose::TakeProfit
                                | OrderPurpose::EmergencyExit
                        )
                    }
                );


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
                Direction::Long => {
                    Side::Sell
                }

                Direction::Short => {
                    Side::Buy
                }
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
        // CAPTURE EXECUTION INSTRUCTIONS
        // ============================================================

        // Capture these before moving their values into the canonical Order.
        //
        // They are also useful for the submission log below.
        let order_type =
            execution.order_type;

        // TEST:
        // Alpaca fractional equity orders require DAY time-in-force.
        // Remove this override once Alpaca execution capability
        // normalization is implemented.
        let time_in_force =
            match venue {
                Venue::Alpaca => {
                    TimeInForce::Day
                }

                Venue::Hyperliquid
                | Venue::InteractiveBrokers => {
                    execution.time_in_force
                }
            };
        // older original
        // let time_in_force =
        //     execution.time_in_force;

        let limit_px =
            execution.limit_px;

        let stop_px =
            execution.stop_px;

        let post_only =
            execution.post_only;


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
        // - OrderId            -> internal engine identity
        // - venue_order_id     -> venue-assigned order identity
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
        // The same ClientOrderId remains attached to this canonical Order for
        // its entire lifecycle.
        let client_order_id =
            ClientOrderId::new();


        // ============================================================
        // EXIT ORDER
        // ============================================================

        let order =
            Order {
                id:
                    order_id,

                venue,

                venue_order_id:
                    None,

                client_order_id,

                strategy_id:
                    strategy_id.clone(),

                trade_id:
                    Some(trade_id),

                symbol:
                    symbol.clone(),

                side,

                purpose:
                    OrderPurpose::Exit,

                // Exit execution behavior comes from the validated canonical
                // ExitOrderRequest.
                order_type,

                time_in_force,

                quantity,

                limit_px,

                stop_px,

                // ====================================================
                // REDUCE-ONLY INVARIANT
                // ====================================================

                // Every directional Trade exit created through this handler
                // MUST reduce existing exposure.
                //
                // This is intentionally Engine-controlled rather than supplied
                // through ExitOrderRequest.
                reduce_only:
                    true,

                post_only,

                filled_quantity:
                    0.0,

                avg_fill_px:
                    None,

                pending_replace:
                    None,

                state:
                    OrderState::PendingSubmit,

                created_ts_ms:
                    ts_ms,

                updated_ts_ms:
                    ts_ms,
            };


        // ============================================================
        // EXIT RESERVATION NOTIONAL
        // ============================================================

        // Exit reservations primarily protect quantity from being closed by
        // multiple simultaneous exit orders.
        //
        // Keep an approximate notional as useful canonical risk metadata.
        //
        // Prefer:
        //
        // 1. current market price
        // 2. average entry price
        // 3. original entry signal price
        //
        // The reservation is quantity-oriented for exit protection, so the
        // limit price itself does not need to redefine established exposure.
        let reference_px =
            self.last_px
                .get(
                    &symbol
                )
                .map(
                    |last| {
                        last.px
                    }
                )
                .or_else(
                    || {
                        self.state
                            .trades
                            .get(trade_id)
                            .and_then(
                                |trade| {
                                    trade.avg_entry_px
                                }
                            )
                    }
                )
                .or_else(
                    || {
                        self.state
                            .trades
                            .get(trade_id)
                            .map(
                                |trade| {
                                    trade.entry_signal_px
                                }
                            )
                    }
                )
                .unwrap_or(
                    0.0
                );


        let reserved_notional =
            quantity
                * reference_px;


        // ============================================================
        // EXIT RISK RESERVATION
        // ============================================================

        let reservation =
            RiskReservation {
                order_id,

                trade_id:
                    Some(trade_id),

                strategy_id:
                    strategy_id.clone(),

                symbol:
                    symbol.clone(),

                purpose:
                    ReservationPurpose::Exit,

                quantity,

                notional:
                    reserved_notional,

                created_ts_ms:
                    ts_ms,

                updated_ts_ms:
                    ts_ms,
            };


        // ============================================================
        // TRANSITION TRADE TO CLOSING
        // ============================================================

        // Closing means an exit is being worked while confirmed exposure may
        // still remain.
        //
        // This state applies equally to:
        //
        // - immediately executing market exits
        // - resting limit exits
        // - partially filled exits
        //
        // Do NOT set Closed here. Only confirmed Fill events can reduce the
        // remaining Trade quantity to zero.
        {
            let Some(trade) =
                self.state
                    .trades
                    .get_mut(
                        trade_id
                    )
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

        self.state
            .orders
            .insert(
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
        // the command.
        //
        // The Order remains PendingSubmit until canonical execution events
        // report what actually happened at the venue.
        //
        // For a resting limit order the typical lifecycle becomes:
        //
        // PendingSubmit
        //     ↓
        // OrderAccepted
        //     ↓
        // OrderWorking
        //     ↓
        // Working
        //     ↓
        // Fill(s) / Cancel / Expire / Replace
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
                .release(
                    order_id
                );


            self.state
                .orders
                .remove(
                    order_id
                );


            if let Some(trade) =
                self.state
                    .trades
                    .get_mut(
                        trade_id
                    )
            {
                if trade.state
                    == TradeState::Closing
                {
                    trade.state =
                        TradeState::Open;
                }
            }


            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE TRADE SUBMIT FAILED | trade_id={} | order_id={} | strategy={} | symbol={} | type={:?} | tif={:?} | limit_px={:?} | stop_px={:?} | post_only={} | venue={:?} | reason={} | error={}",
                    trade_id,
                    order_id,
                    strategy_id,
                    symbol,
                    order_type,
                    time_in_force,
                    limit_px,
                    stop_px,
                    post_only,
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
                "[ENGINE] CLOSE TRADE SUBMITTED | trade_id={} | order_id={} | strategy={} | symbol={} | side={:?} | type={:?} | tif={:?} | qty={} | limit_px={:?} | stop_px={:?} | post_only={} | reduce_only=true | venue={:?} | reason={}",
                trade_id,
                order_id,
                strategy_id,
                symbol,
                side,
                order_type,
                time_in_force,
                quantity,
                limit_px,
                stop_px,
                post_only,
                venue,
                reason,
            ),
        );
    }
}