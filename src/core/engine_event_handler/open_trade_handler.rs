use std::time::{
    Instant,
    SystemTime,
};

use crate::core::{
    engine_state::{
        Engine,
        ExecutionMode,
        TradingPhase,
    },
    trade_intent::TradeIntent,
    trades::{
        PendingOpen,
        Trade,
        TradeStatus,
    },
    exec::{
        ExecCommand,
        Direction
    }
};

use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_error_channel_id,
        get_discord_error_channel_id,
        get_slack_trades_channel_id,
        get_discord_trades_channel_id,
    }
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};

use crate::shared::print_log;

// ============================================================
// HANDLE OPEN LIVE TRADE
// ============================================================

impl Engine {
    /// Handles a request to open a live or simulated trade.
    ///
    /// Validates the incoming intent against engine-level trading gates,
    /// then either creates an immediate simulated trade or submits a live
    /// execution command to the exchange adapter.
    ///
    /// # Arguments
    ///
    /// * `intent` - Trade intent containing the strategy, symbol, direction,
    ///   sizing, price, risk configuration, and decision timestamp.
    pub(crate) fn handle_open_trade(
        &mut self,
        intent: TradeIntent,
    ) {
        self.print_trade_intent(
            "LIVE",
            &intent,
        );

        // ============================================================
        // EXECUTION MODE GATE
        // ============================================================

        // OpenTradeIntent is only valid in Live or Simulation mode.
        if !matches!(
            self.execution_mode,
            ExecutionMode::Live | ExecutionMode::Simulation
        ) {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                    symbol={} | strategy={} | \
                    invalid execution mode={:?}",
                    intent.symbol,
                    intent.strategy,
                    self.execution_mode,
                ),
            );

            return;
        }

        // ============================================================
        // CLOSING TRADES GATE
        // ============================================================

        // Prevent new entries while the engine is flattening positions.
        if self.trading_phase == TradingPhase::ClosingTrades {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                     strategy={} | symbol={} | \
                     phase=ClosingTrades",
                    intent.strategy,
                    intent.symbol,
                ),
            );

            return;
        }

        // ============================================================
        // COOLDOWN GATE
        // ============================================================

        if let Some(last_ts) = self.last_decision_ts.get(&intent.symbol) {
            let elapsed_ms =
                intent.ts_ms - *last_ts;

            if elapsed_ms < self.cfg.cooldown_ms {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[ENGINE LIVE][GATE] REJECT | \
                         symbol={} | strategy={} | \
                         cooldown={}ms | elapsed={}ms",
                        intent.symbol,
                        intent.strategy,
                        self.cfg.cooldown_ms,
                        elapsed_ms,
                    ),
                );

                return;
            }
        }

        // ============================================================
        // GLOBAL POSITION LIMIT GATE
        // ============================================================

        // Live trading must count BOTH:
        //
        // - already open/closing positions
        // - opening orders currently pending at the exchange
        //
        // Otherwise several rapid orders could exceed max_gross_positions
        // before their final fill events reach the engine.
        let total_booked = self.booked_positions();

        if total_booked >= self.cfg.max_gross_positions {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                     max_gross_positions reached | \
                     max={} | booked={} | open={} | pending_opens={}",
                    self.cfg.max_gross_positions,
                    total_booked,
                    self.get_open_trade_count(),
                    self.pending_opens.len(),
                ),
            );

            return;
        }

        // ============================================================
        // SYMBOL OWNERSHIP / DUPLICATE POSITION GATE
        // ============================================================

        let owner = self.owner_by_symbol.get(&intent.symbol).cloned();

        let open_for_symbol =
            *self.open_count_by_symbol
                .get(&intent.symbol)
                .unwrap_or(&0);

        if open_for_symbol > 0 || owner.is_some() {
            let owner_name =
                owner.unwrap_or_else(|| "<unknown>".to_string());

            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                     symbol={} already owned/open | \
                     owner={} | open_count={}",
                    intent.symbol,
                    owner_name,
                    open_for_symbol,
                ),
            );

            return;
        }

        // ============================================================
        // PRICE VALIDATION
        // ============================================================

        if !intent.price.is_finite()
            || intent.price <= 0.0
        {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                     invalid price={} | symbol={} | strategy={}",
                    intent.price,
                    intent.symbol,
                    intent.strategy,
                ),
            );

            return;
        }

        // ============================================================
        // TRADE AMOUNT VALIDATION
        // ============================================================

        if !intent.trade_amount_usd.is_finite()
            || intent.trade_amount_usd <= 0.0
        {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                     invalid trade_amount_usd={} | \
                     symbol={} | strategy={}",
                    intent.trade_amount_usd,
                    intent.symbol,
                    intent.strategy,
                ),
            );

            return;
        }

        // ============================================================
        // ORDER SIZE
        // ============================================================

        // This bots Alpaca entries are submitted using dollar notional instead of
        // calculating share quantity from the strategy's observed price.
        //
        // Alpaca determines the final fractional share quantity from the actual
        // market execution price, and the WebSocket fill events later provide the
        // true filled quantity and average execution price.
        //
        // Example:
        //   trade_amount_usd = $100
        //   actual fill price = $320
        //   final filled quantity ≈ 0.3125 shares
        let order_notional = intent.trade_amount_usd;

        if !order_notional.is_finite() || order_notional <= 0.0 {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                    invalid order_notional={} | symbol={} | strategy={}",
                    order_notional,
                    intent.symbol,
                    intent.strategy,
                ),
            );

            return;
        }

        // ============================================================
        // POSITION SIZE
        // ============================================================

        // Simulation needs a calculated quantity because it creates the
        // filled trade immediately without sending an order to an exchange.
        let qty: f64 = intent.trade_amount_usd / intent.price;

        if !qty.is_finite() || qty <= 0.0 {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE LIVE][GATE] REJECT | \
                    invalid qty={} | symbol={} | strategy={}",
                    qty,
                    intent.symbol,
                    intent.strategy,
                ),
            );

            return;
        }


        // ============================================================
        // SIMULATION MODE
        // ============================================================

        // Simulation uses the live market-data path but does not submit
        // an order to the exchange. The trade is filled immediately
        // using the current price contained in the intent.
        if self.execution_mode
            == ExecutionMode::Simulation
        {
            self.open_simulated_trade_from_intent(
                intent,
                qty,
            );

            return;
        }

        // ============================================================
        // LIVE EXECUTION
        // ============================================================

        let is_long = intent.is_long;

        let side =
            if is_long {
                Direction::Long
            } else {
                Direction::Short
            };

        let cid =
            format!(
                "{}-{}-{}",
                intent.strategy,
                intent.symbol,
                intent.ts_ms,
            );


        // ============================================================
        // STORE PENDING OPEN ORDER
        // ============================================================

        // Store the pending open before sending the command to the adapter.
        // The Alpaca order id is unknown until the REST request succeeds.
        // Partial fill events will update filled_qty and avg_fill_px.
        // The pending open is removed only after the final OpenFill.
        self.pending_opens.insert(
            cid.clone(),
            PendingOpen {
                cid: cid.clone(),
                oid: None,
                symbol: intent.symbol.clone(),
                strategy: intent.strategy.clone(),
                is_long,
                requested_qty: qty,
                filled_qty: 0.0,
                avg_fill_px: None,
                signal_price: intent.price,
                execution_ids: Default::default(),
                ts_ms: intent.ts_ms,
                trade_amount_usd: intent.trade_amount_usd,
                exit_profit_percent: intent.exit_profit_percent,
                exit_loss_percent: intent.exit_loss_percent,
                emergency_exit_percent: intent.emergency_exit_percent,
                min_sl_delay: intent.min_sl_delay,
                enable_trade_duration: intent.enable_trade_duration,
                trade_duration_secs: intent.trade_duration_secs,
            },
        );

        // ============================================================
        // RESERVE SYMBOL
        // ============================================================

        // Reserve ownership while the opening order is pending so another
        // strategy event cannot submit a second order for this symbol.
        self.owner_by_symbol.insert(
            intent.symbol.clone(),
            intent.strategy.clone(),
        );

        // Record decision time when the order is accepted for submission.
        self.last_decision_ts.insert(
            intent.symbol.clone(),
            intent.ts_ms,
        );

        // ============================================================
        // SEND LIVE ORDER
        // ============================================================

        let symbol_for_error = intent.symbol.clone();

        let cmd =
           ExecCommand::Place {
            cid: cid.clone(),
            symbol: intent.symbol.clone(),
            side,
            notional: order_notional,
        };

        if let Err(err) =
            self.tx_exec.send(cmd)
        {
            // Submission failed, so undo the state that reserved
            // this order and symbol.
            self.pending_opens.remove(&cid);

            self.owner_by_symbol.remove(&intent.symbol);

            let message = format!(
                "🚨 EXECUTION ERROR – `{}`\n\
                Failed to send ExecCommand::Place\n\
                Symbol: `{}`\n\
                Strategy: `{}`\n\
                Error: `{}`\n\
                Time: `{}`",
                get_bot_name(),
                symbol_for_error,
                intent.strategy,
                err,
                get_local_timestamp(),
            );

            send_remote_message(
                &self.cfg,
                message,
                Some(get_slack_error_channel_id()),
                Some(get_discord_error_channel_id()),
            );

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE LIVE] ERROR | \
                     failed to send ExecCommand::Place | \
                     symbol={} | strategy={} | error={}",
                    symbol_for_error,
                    intent.strategy,
                    err,
                ),
            );

            return;
        }

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE LIVE] ORDER SUBMITTED | \
                cid={} | \
                symbol={} | \
                strategy={} | \
                direction={} | \
                price=${:.4} | \
                notional=${:.2} | \
                booked_positions={}",
                cid,
                intent.symbol,
                intent.strategy,
                if is_long {
                    "LONG"
                } else {
                    "SHORT"
                },
                intent.price,
                intent.trade_amount_usd,
                self.booked_positions(),
            ),
        );
    }


    /// Creates and records an immediately filled simulated trade.
    ///
    /// Simulation uses the live strategy and market-data path but does not
    /// submit an order to the exchange. The intent price is used as the
    /// simulated execution price.
    ///
    /// # Arguments
    ///
    /// * `intent` - Validated live trade intent used to create the position.
    /// * `qty` - Calculated unsigned position quantity.
    pub(crate) fn open_simulated_trade_from_intent(
        &mut self,
        intent: TradeIntent,
        qty: f64,
    ) {
        let now_mono = Instant::now();
        let now_wall = SystemTime::now();
        let is_long = intent.is_long;

        let signed_qty =
            if is_long {
                qty
            } else {
                -qty
            };

        let notional = intent.trade_amount_usd;

        let id =
            format!(
                "{}-{}-{}-SIM",
                intent.strategy,
                intent.symbol,
                intent.ts_ms,
            );

        let cid =
            format!(
                "sim-{}",
                id
            );

        let oid =
            format!(
                "sim-{}",
                id
            );

        let max_duration =
            if intent.enable_trade_duration {
                match intent.trade_duration_secs {
                    Some(secs) => {
                        format!("{}s", secs)
                    }

                    None => {
                        "configured: unknown".to_string()
                    }
                }
            } else {
                "disabled".to_string()
            };


        // ============================================================
        // CREATE SIMULATED TRADE
        // ============================================================

        let trade =
            Trade {
                id: id.clone(),
                symbol: intent.symbol.clone(),
                is_long,
                qty: signed_qty,
                executed_entry_avg_px: intent.price,
                executed_exit_avg_px: None,
                entry_signal_price: intent.price,
                exit_signal_price: None,
                oid: oid.clone(),
                cid: cid.clone(),
                strategy: intent.strategy.clone(),
                opened_ms: intent.ts_ms,
                unrealized_pnl: 0.0,
                status: TradeStatus::Open,
                trade_amount_usd: intent.trade_amount_usd,
                exit_profit_percent: intent.exit_profit_percent,
                exit_loss_percent: intent.exit_loss_percent,
                emergency_exit_percent: intent.emergency_exit_percent,
                min_sl_delay: intent.min_sl_delay,
                enable_trade_duration: intent.enable_trade_duration,
                trade_duration_secs: intent.trade_duration_secs,
                entry_time: Some(now_mono),
                entry_wall_time: Some(now_wall),
                is_simulated: true,
                close_cid: None,
                close_oid: None,
            };


        // ============================================================
        // STORE TRADE
        // ============================================================

        self.trades.upsert(trade);

        // ============================================================
        // SIM TRADE OPENED NOTIFICATION
        // ============================================================

        let message = format!(
            "🧪 SIM TRADE OPENED – `{}`\n\
            Symbol: `{}`\n\
            Side: `{}`\n\
            Size: `{:.6}`\n\
            USD: `${:.2}`\n\
            Notional: `${:.2}`\n\
            Executed Price: `${:.6}`\n\
            Signal Price: `${:.6}`\n\
            Max Duration: `{}`\n\
            Strategy: `{}`\n\
            CID: `{}`\n\
            OID: `{}`\n\
            Time: `{}`",
            get_bot_name(),
            intent.symbol,
            if intent.is_long {
                "LONG"
            } else {
                "SHORT"
            },
            signed_qty.abs(),
            intent.trade_amount_usd,
            signed_qty.abs() * intent.price,
            intent.price,
            intent.price,
            max_duration,
            intent.strategy,
            cid,
            oid,
            get_local_timestamp(),
        );

        send_remote_message(
            &self.cfg,
            message,
            Some(get_slack_trades_channel_id()),
            Some(get_discord_trades_channel_id()),
        );

        // ============================================================
        // RECORD ENGINE STATE
        // ============================================================

        self.owner_by_symbol.insert(
            intent.symbol.clone(),
            intent.strategy.clone(),
        );

        *self.open_count_by_symbol
            .entry(intent.symbol.clone())
            .or_default() += 1;

        self.last_decision_ts.insert(
            intent.symbol.clone(),
            intent.ts_ms,
        );

        // ============================================================
        // SIMULATION OPEN LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE SIMULATION] ENTRY TRADE | \
                id={} | \
                symbol={} | \
                strategy={} | \
                direction={} | \
                price=${:.4} | \
                qty={:.6} | \
                notional=${:.2} | \
                max_duration={} | \
                open_positions={}",
                id,
                intent.symbol,
                intent.strategy,
                if is_long {
                    "LONG"
                } else {
                    "SHORT"
                },
                intent.price,
                signed_qty,
                notional,
                max_duration,
                self.get_open_trade_count(),
            ),
        );
    }
}