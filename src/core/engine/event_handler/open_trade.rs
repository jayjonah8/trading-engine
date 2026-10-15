#![allow(dead_code)]

use crate::core::{
    engine::engine::Engine,
    engine::state::ExecutionMode,

    execution::{
        ClientOrderId,
        ExecCommand,
        Order,
        OrderPurpose,
        OrderState,
    },

    risk::{
        checks::{
            check_active_asset_limit,
            check_active_trade_limit,
            check_active_trades_per_strategy,
            check_active_trades_per_symbol,
            check_gross_notional,
            check_order_notional,
            check_symbol_notional,
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
use crate::core::venue::validate_order;


// TODO:
// Add detailed TradeIntent printout for the new execution architecture.

// TODO:
// Add any remaining strategy ownership / cooldown admission rules here
// if those policies remain part of the final engine behavior.

// TODO:
// Add structured rejection/output events later so failed intents can be
// surfaced to metrics, persistence, notifications, or strategies without
// relying only on logs.

// TODO:
// Add shared venue capability validation before order submission.
//
// This should validate execution behavior such as:
// - supported order types
// - supported time-in-force values
// - post-only support
// - reduce-only support
//
// Keep these rules outside open_trade.rs so this handler remains
// venue-independent.


// ============================================================
// HANDLE OPEN TRADE
// ============================================================

impl Engine {
    /// Handles a strategy request to create a new directional Trade.
    ///
    /// TradeIntent
    ///     ↓
    /// validate
    ///     ↓
    /// strategy/symbol ownership gate
    ///     ↓
    /// count-based risk checks
    ///     ↓
    /// notional/exposure risk checks
    ///     ↓
    /// normalize executable quantity/prices
    ///     ↓
    /// create Trade + Order + RiskReservation
    ///     ↓
    /// commit authoritative Engine state
    ///     ↓
    /// ExecCommand::SubmitOrder
    ///     ↓
    /// execution router
    ///     ↓
    /// venue adapter
    pub fn handle_open_trade(&mut self, intent: TradeIntent) {
        // ============================================================
        // VALIDATE INTENT
        // ============================================================

        if let Err(err) = intent.validate() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // TOTAL ACTIVE TRADE LIMIT
        // ============================================================

        if let Err(err) = check_active_trade_limit(&self.state.risk.limits, &self.state.trades) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // ACTIVE ASSET LIMIT
        // ============================================================

        if let Err(err) = check_active_asset_limit(
            &self.state.risk.limits,
            &self.state.trades,
            &intent.symbol,
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // ACTIVE TRADES PER SYMBOL LIMIT
        // ============================================================

        if let Err(err) = check_active_trades_per_symbol(
            &self.state.risk.limits,
            &self.state.trades,
            &intent.symbol,
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // ACTIVE TRADES PER STRATEGY LIMIT
        // ============================================================

        if let Err(err) = check_active_trades_per_strategy(
            &self.state.risk.limits,
            &self.state.trades,
            &intent.strategy_id,
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // SINGLE ORDER NOTIONAL LIMIT
        // ============================================================

        if let Err(err) = check_order_notional(&self.state.risk.limits, intent.target_notional) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // CURRENT GROSS POSITION NOTIONAL
        // ============================================================

        // Established exposure is valued using the latest known market price.
        //
        // If a current market price is unavailable, average entry price is
        // used as a fallback so existing exposure is still represented in
        // admission checks.
        let current_gross_notional = self.state.positions.iter().map(|(_, position)| {
            let px = self.last_px
                .get(&position.key.symbol)
                .map(|last| last.px)
                .or(position.avg_entry_px)
                .unwrap_or(0.0);

            position.absolute_quantity() * px
        }).sum::<f64>();


        // ============================================================
        // GROSS NOTIONAL LIMIT
        // ============================================================

        // check_gross_notional() includes:
        //
        // established Position exposure
        //     +
        // all active Entry RiskReservations
        //     +
        // this proposed Trade
        if let Err(err) = check_gross_notional(
            &self.state.risk.limits,
            &self.state.risk.reservations,
            current_gross_notional,
            intent.target_notional,
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // CURRENT SYMBOL POSITION NOTIONAL
        // ============================================================

        let current_symbol_notional = self.state.positions.iter()
            .filter(|(_, position)| position.key.symbol == intent.symbol)
            .map(|(_, position)| {
                let px = self.last_px
                    .get(&position.key.symbol)
                    .map(|last| last.px)
                    .or(position.avg_entry_px)
                    .unwrap_or(intent.signal_px);

                position.absolute_quantity() * px
            })
            .sum::<f64>();


        // ============================================================
        // RESERVED SYMBOL NOTIONAL
        // ============================================================

        let reserved_symbol_notional = self.state.risk.reservations.entry_notional_for_symbol(&intent.symbol);


        // ============================================================
        // SYMBOL NOTIONAL LIMIT
        // ============================================================

        if let Err(err) = check_symbol_notional(
            &self.state.risk.limits,
            &intent.symbol,
            current_symbol_notional,
            reserved_symbol_notional,
            intent.target_notional,
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, err,
                ),
            );

            return;
        }


        // ============================================================
        // EXECUTABLE QUANTITY
        // ============================================================

        // Convert the strategy's requested notional exposure into a desired
        // asset quantity:
        //
        //     raw_quantity = target_notional / signal_price
        //
        // VenueState then converts that desired quantity into the largest
        // executable quantity allowed by the destination venue/instrument
        // without increasing requested exposure.
        //
        // Side + OrderPurpose provide execution context for venue rules that
        // cannot be determined from quantity alone.
        //
        // Alpaca:
        // - long entry  -> fractional quantity supported
        // - short entry -> fractional quantity normalized to whole shares
        //
        // IBKR:
        // - quantities are currently normalized to whole shares
        //
        // Hyperliquid:
        // - fractional behavior and precision come from InstrumentState
        let side = intent.entry_side();
        let purpose = OrderPurpose::Entry;
        let raw_quantity = intent.target_notional / intent.signal_px;

        let quantity = match self.cfg.execution_mode {
            ExecutionMode::Live | ExecutionMode::Paper => {
                match self.venue_state.normalize_quantity(
                    self.cfg.execution_venue,
                    &intent.symbol,
                    side,
                    purpose,
                    raw_quantity,
                ) {
                    Ok(quantity) => quantity,

                    Err(error) => {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | side={:?} | raw_quantity={} | error={:?}",
                                intent.strategy_id, intent.symbol, side, raw_quantity, error,
                            ),
                        );

                        return;
                    }
                }
            }

            ExecutionMode::Simulation | ExecutionMode::Backtest => {
                raw_quantity
            }
        };


        // ============================================================
        // LIMIT PRICE
        // ============================================================

        // Live and paper execution normalize strategy-provided prices using
        // canonical venue/instrument rules.
        //
        // Simulation and backtest execution preserve the requested price directly.
        // Simulated venue-specific price rules can be added later if required.
        let limit_px = match self.cfg.execution_mode {
            ExecutionMode::Live | ExecutionMode::Paper => {
                match intent.limit_px {
                    Some(raw_limit_px) => {
                        match self.venue_state.normalize_price(
                            self.cfg.execution_venue,
                            &intent.symbol,
                            raw_limit_px,
                        ) {
                            Ok(limit_px) => Some(limit_px),

                            Err(error) => {
                                print_log(
                                    self.debug_mode,
                                    true,
                                    format!(
                                        "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | raw_limit_px={} | error={:?}",
                                        intent.strategy_id, intent.symbol, raw_limit_px, error,
                                    ),
                                );

                                return;
                            }
                        }
                    }

                    None => None,
                }
            }

            ExecutionMode::Simulation | ExecutionMode::Backtest => {
                intent.limit_px
            }
        };


        // ============================================================
        // STOP PRICE
        // ============================================================

        // Live and paper execution normalize stop prices using the same
        // canonical venue/instrument price rules as limit prices.
        //
        // Simulation and backtest execution preserve the requested price directly.
        let stop_px = match self.cfg.execution_mode {
            ExecutionMode::Live | ExecutionMode::Paper => {
                match intent.stop_px {
                    Some(raw_stop_px) => {
                        match self.venue_state.normalize_price(
                            self.cfg.execution_venue,
                            &intent.symbol,
                            raw_stop_px,
                        ) {
                            Ok(stop_px) => Some(stop_px),

                            Err(error) => {
                                print_log(
                                    self.debug_mode,
                                    true,
                                    format!(
                                        "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | raw_stop_px={} | error={:?}",
                                        intent.strategy_id, intent.symbol, raw_stop_px, error,
                                    ),
                                );

                                return;
                            }
                        }
                    }

                    None => None,
                }
            }

            ExecutionMode::Simulation | ExecutionMode::Backtest => {
                intent.stop_px
            }
        };


        // ============================================================
        // TIME IN FORCE
        // ============================================================

        // Live and paper execution apply proven venue-specific TIF rules.
        //
        // Simulation and backtest execution preserve the strategy-requested TIF.
        // Simulated venue-specific TIF behavior can be added later if required.
        let time_in_force = match self.cfg.execution_mode {
            ExecutionMode::Live | ExecutionMode::Paper => {
                self.venue_state.normalize_time_in_force(
                    self.cfg.execution_venue,
                    quantity,
                    intent.time_in_force,
                )
            }

            ExecutionMode::Simulation | ExecutionMode::Backtest => {
                intent.time_in_force
            }
        };


        // ============================================================
        // IDS
        // ============================================================

        let trade_id = self.state.allocate_trade_id();
        let order_id = self.state.allocate_order_id();
        let client_order_id = ClientOrderId::new();


        // ============================================================
        // TRADE
        // ============================================================

        let trade = Trade {
            id: trade_id,
            strategy_id: intent.strategy_id.clone(),
            symbol: intent.symbol.clone(),
            direction: intent.direction,
            state: TradeState::Opening,
            quantity: 0.0,
            exited_quantity: 0.0,
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
            entry_fees: 0.0,
            exit_fees: 0.0,
            allocated_entry_fees: 0.0,
            created_ts_ms: intent.ts_ms,
            opened_ts_ms: None,
            closed_ts_ms: None,
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
            side,
            purpose,
            order_type: intent.order_type,
            time_in_force,
            quantity,
            limit_px,
            stop_px,
            reduce_only: false,
            post_only: intent.post_only,
            filled_quantity: 0.0,
            avg_fill_px: None,
            pending_replace: None,
            state: OrderState::PendingSubmit,
            created_ts_ms: intent.ts_ms,
            updated_ts_ms: intent.ts_ms,
        };


        // ============================================================
        // VALIDATE CANONICAL ORDER
        // ============================================================

        if let Err(error) = validate_order(&order) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE REJECTED | strategy={} | symbol={} | error={:?}",
                    intent.strategy_id, intent.symbol, error,
                ),
            );

            return;
        }


        // ============================================================
        // RISK RESERVATION
        // ============================================================

        // Reserve proposed exposure before venue submission so another
        // TradeIntent cannot consume the same risk capacity while this Order
        // is still pending or working.
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

        // Commit canonical state before sending execution so immediately
        // arriving venue events can always resolve OrderId and TradeId.
        self.state.trades.insert(trade);
        self.state.orders.insert(order.clone());
        self.state.risk.reservations.insert(reservation);


        // ============================================================
        // SUBMIT ORDER
        // ============================================================

        if let Err(err) = self.tx_exec.send(ExecCommand::SubmitOrder { order }) {
            // The command never reached the execution router, so roll back all
            // state created for this submission.
            self.state.risk.reservations.release(order_id);
            self.state.orders.remove(order_id);
            self.state.trades.remove(trade_id);

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] OPEN TRADE SUBMIT FAILED | trade_id={} | order_id={} | strategy={} | symbol={} | error={}",
                    trade_id, order_id, intent.strategy_id, intent.symbol, err,
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
                "[ENGINE] OPEN TRADE SUBMITTED | trade_id={} | order_id={} | strategy={} | symbol={} | side={:?} | type={:?} | tif={:?} | qty={} | limit_px={:?} | stop_px={:?} | notional=${:.2} | venue={:?}",
                trade_id,
                order_id,
                intent.strategy_id,
                intent.symbol,
                side,
                intent.order_type,
                time_in_force,
                quantity,
                limit_px,
                stop_px,
                intent.target_notional,
                self.cfg.execution_venue,
            ),
        );
    }
}