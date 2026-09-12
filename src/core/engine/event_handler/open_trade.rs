use crate::core::{
    engine::engine::Engine,
    execution::{
        ExecCommand,
        Order,
        OrderPurpose,
        OrderState,
    },
    risk::{
        checks::{
            check_active_trade_limit,
            check_order_notional,
        },
        ReservationPurpose,
        RiskReservation,
    },
    trade_intent::TradeIntent,
    trades::{
        Trade,
        TradeState,
    },
};

use crate::shared::print_log;

// TODO:
// Add detail Intent printout thats formatted and detailed 
// for the new archictecture. 

// TODO:
// Add any remaining strategy ownership / cooldown admission rules here
// if those policies remain part of the final engine behavior.

// TODO:
// Add max_gross_notional and max_symbol_notional checks once
// canonical position valuation is available from Positions.

// TODO:
// Replace raw `target_notional / signal_px` sizing with the final
// canonical sizing/normalization path once Quantity/Price and venue
// instrument constraints are implemented.

// TODO:
// Replace the temporary client_order_id format with the final
// canonical client-order ID scheme if needed.

// TODO:
// Add structured rejection/output events later so failed intents can
// be surfaced to metrics, persistence, notifications, or strategies
// without relying only on debug logs. Just print for now and add 
// remote notifications when the lifecyle is complete for atleast 2 venues

impl Engine {
    /// Handles a strategy request to create a new directional trade.
    ///
    /// TradeIntent
    ///     -> validation
    ///     -> engine/risk checks
    ///     -> quantity calculation
    ///     -> Trade + Order ID allocation
    ///     -> Trade creation (Opening)
    ///     -> RiskReservation creation
    ///     -> Order creation (PendingSubmit)
    ///     -> authoritative state insertion
    ///     -> ExecCommand::SubmitOrder
    pub fn handle_open_trade(
        &mut self,
        intent: TradeIntent,
    ) {

        // ============================================================
        // VALIDATE INTENT
        // ============================================================

        if let Err(err) = intent.validate() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id,
                    intent.symbol,
                    err,
                ),
            );

            return;
        }
 

        // ============================================================
        // ACTIVE STRATEGY TRADE GATE
        // ============================================================

        // Prevent the same strategy from accidentally creating another
        // independent logical Trade for this symbol while one is already
        // Opening, Open, or Closing.
        //
        // This does NOT restrict the number of market, limit, stop, or exit
        // Orders that may belong to the existing Trade. One Trade may own
        // many simultaneous Orders throughout its execution lifecycle.
        if self.state.trades.has_active_trade_for_strategy_symbol(
            &intent.strategy_id,
            &intent.symbol,
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | reason=active strategy trade already exists",
                    intent.strategy_id,
                    intent.symbol,
                ),
            );

            return;
        }


        // ============================================================
        // RISK CHECKS
        // ============================================================

        if let Err(err) =
            check_active_trade_limit(
                &self.state.risk.limits,
                &self.state.trades,
            )
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id,
                    intent.symbol,
                    err,
                ),
            );

            return;
        }

        if let Err(err) =
            check_order_notional(
                &self.state.risk.limits,
                intent.target_notional,
            )
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id,
                    intent.symbol,
                    err,
                ),
            );

            return;
        }


        // ============================================================
        // REQUESTED QUANTITY
        // ============================================================

        // Calculate the engine's desired quantity from the strategy notional.
        //
        // Venue-specific quantity rules such as precision, lot size, and
        // fractional-share support belong at the execution boundary.
        let quantity =
            intent.target_notional
                / intent.signal_px;


        // ============================================================
        // IDS
        // ============================================================

        let trade_id =
            self.state.allocate_trade_id();

        let order_id =
            self.state.allocate_order_id();

        let client_order_id =
            format!(
                "{}-{}",
                intent.strategy_id,
                order_id,
            );


        // ============================================================
        // TRADE
        // ============================================================

        // The logical trade exists immediately, but it has no established
        // quantity until confirmed entry fills arrive.
        let trade = Trade {
            id: trade_id,
            strategy_id: intent.strategy_id.clone(),
            symbol: intent.symbol.clone(),
            direction: intent.direction,
            state: TradeState::Opening,
            quantity: 0.0,
            avg_entry_px: None,
            avg_exit_px: None,
            entry_signal_px: intent.signal_px,
            exit_signal_px: None,
            target_notional: intent.target_notional,
            take_profit_percent: intent.take_profit_percent,
            stop_loss_percent: intent.stop_loss_percent,
            emergency_stop_percent: intent.emergency_stop_percent,
            stop_loss_delay_secs: intent.stop_loss_delay_secs,
            max_duration_secs: intent.max_duration_secs,
            created_ts_ms: intent.ts_ms,
            opened_ts_ms: None,
            closed_ts_ms: None,
            entry_time: None,
            entry_wall_time: None,
        };

        // ============================================================
        // ORDER
        // ============================================================

        let order = Order {
            id: order_id,
            venue: self.cfg.execution_venue,
            venue_order_id: None,
            client_order_id,
            strategy_id: intent.strategy_id.clone(),
            trade_id: Some(trade_id),
            symbol: intent.symbol.clone(),
            side: intent.entry_side(),
            purpose: OrderPurpose::Entry,
            order_type: intent.order_type,
            time_in_force: intent.time_in_force,
            quantity,
            limit_px: intent.limit_px,
            stop_px: intent.stop_px,
            reduce_only: false,
            post_only: intent.post_only,
            filled_quantity: 0.0,
            avg_fill_px: None,
            state: OrderState::PendingSubmit,
            created_ts_ms: intent.ts_ms,
            updated_ts_ms: intent.ts_ms,
        };


        // ============================================================
        // RISK RESERVATION
        // ============================================================

        // Reserve the requested exposure before sending the order so another
        // strategy decision cannot consume the same risk capacity.
        let reservation = RiskReservation {
            order_id,
            trade_id: Some(trade_id),
            strategy_id: intent.strategy_id.clone(),
            symbol: intent.symbol.clone(),
            purpose: ReservationPurpose::Entry,
            quantity,
            notional: intent.target_notional,
            created_ts_ms: intent.ts_ms,
            updated_ts_ms: intent.ts_ms,
        };


        // ============================================================
        // COMMIT ENGINE STATE
        // ============================================================

        self.state.trades.insert(trade);
        self.state.orders.insert(order.clone());
        self.state.risk.reservations.insert(reservation);

        // ============================================================
        // SUBMIT ORDER
        // ============================================================


        // A successful channel send only means the execution adapter received
        // the command. The order remains PendingSubmit until canonical
        // ExecEvents report what actually happened at the venue.
        if let Err(err) =
            self.tx_exec.send(
                ExecCommand::SubmitOrder {
                    order,
                }
            )
        {
            // The command never reached the execution adapter, so roll back
            // all authoritative state created for this submission.
            self.state
                .risk
                .reservations
                .release(order_id);

            self.state
                .orders
                .remove(order_id);

            self.state
                .trades
                .remove(trade_id);

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE SUBMIT FAILED | trade_id={} | order_id={} | symbol={} | error={}",
                    trade_id,
                    order_id,
                    intent.symbol,
                    err,
                ),
            );

            return;
        }


        // ============================================================
        // SUBMITTED
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] OPEN TRADE SUBMITTED | trade_id={} | order_id={} | strategy={} | symbol={} | side={:?} | type={:?} | qty={} | notional=${:.2} | venue={:?}",
                trade_id,
                order_id,
                intent.strategy_id,
                intent.symbol,
                intent.entry_side(),
                intent.order_type,
                quantity,
                intent.target_notional,
                self.cfg.execution_venue,
            ),
        );
    }
}