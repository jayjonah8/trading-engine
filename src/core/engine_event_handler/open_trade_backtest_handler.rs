use crate::core::{
    engine_state::{
        Engine,
        ExecutionMode,
        TradingPhase,
    },
    trade_intent::TradeIntent,
    trades::{
        Trade,
        TradeStatus,
    },
};

use crate::shared::print_log;

// ============================================================
// HANDLE OPEN BACKTEST TRADE
// ============================================================

impl Engine {
    
    /// Handles a request to open a simulated trade during a backtest.
    ///
    /// Validates the incoming intent against engine-level trading gates
    /// before creating the simulated position.
    pub(crate) fn handle_open_backtest_trade(
        &mut self,
        intent: TradeIntent,
    ) {
        self.print_trade_intent(
            "BACKTEST",
            &intent,
        );

        // ============================================================
        // EXECUTION MODE GATE
        // ============================================================

        // OpenBackTestTrade should only ever be processed while
        // the engine itself is running in backtest mode.
        if self.execution_mode
            != ExecutionMode::Backtest
        {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE BACKTEST][GATE] REJECT | \
                     symbol={} | strategy={} | \
                     engine is not in Backtest mode",
                    intent.symbol,
                    intent.strategy,
                ),
            );

            return;
        }

        // ============================================================
        // CLOSING TRADES GATE
        // ============================================================

        // Do not accept new positions while the engine is intentionally
        // flattening / closing existing positions.
        if self.trading_phase
            == TradingPhase::ClosingTrades
        {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE BACKTEST][GATE] REJECT | \
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

        if let Some(last_ts) =
            self.last_decision_ts
                .get(&intent.symbol)
        {
            let elapsed_ms =
                intent.ts_ms - *last_ts;

            if elapsed_ms
                < self.cfg.cooldown_ms
            {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[ENGINE BACKTEST][GATE] REJECT | \
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

        // Backtest entries fill immediately, so there are no PendingOpen
        // orders to reserve. Open and Closing trades still consume a
        // position slot until they are fully closed.
        let open_positions =
            self.trades.count_open();

        if open_positions
            >= self.cfg.max_gross_positions
        {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE BACKTEST][GATE] REJECT | \
                     max_gross_positions reached | \
                     max={} | open={}",
                    self.cfg.max_gross_positions,
                    open_positions,
                ),
            );

            return;
        }

        // ============================================================
        // SYMBOL OWNERSHIP / DUPLICATE POSITION GATE
        // ============================================================

        let owner =
            self.owner_by_symbol
                .get(&intent.symbol)
                .cloned();

        let open_for_symbol =
            *self.open_count_by_symbol
                .get(&intent.symbol)
                .unwrap_or(&0);

        if open_for_symbol > 0
            || owner.is_some()
        {
            let owner_name =
                owner.unwrap_or_else(
                    || "<unknown>".to_string()
                );

            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE BACKTEST][GATE] REJECT | \
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
                    "[ENGINE BACKTEST][GATE] REJECT | \
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
                    "[ENGINE BACKTEST][GATE] REJECT | \
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
        // POSITION SIZE
        // ============================================================

        // Backtest entries are filled immediately at the historical price
        // contained in the TradeIntent, so quantity can be calculated here.
        //
        // Example:
        //   trade_amount_usd = $100
        //   historical fill price = $200
        //   quantity = 0.5 shares
        let qty =
            intent.trade_amount_usd
                / intent.price;

        if !qty.is_finite()
            || qty <= 0.0
        {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE BACKTEST][GATE] REJECT | \
                     invalid qty={} | symbol={} | strategy={}",
                    qty,
                    intent.symbol,
                    intent.strategy,
                ),
            );

            return;
        }

        // ============================================================
        // OPEN BACKTEST TRADE
        // ============================================================

        // All engine gates have passed.
        //
        // Backtest execution is immediate, so no PendingOpen or exchange
        // execution lifecycle is required before creating the Trade.
        self.open_backtest_trade_from_intent(
            intent,
            qty,
        );
    }


    /// Creates and records a simulated trade from an accepted backtest intent.
    ///
    /// This function is called only after the intent has passed all engine
    /// gates and validation checks. It owns the simulated fill, Trade creation,
    /// and engine state updates associated with the new backtest position.
    ///
    /// # Arguments
    ///
    /// * `intent` - Validated trade intent used to create the backtest position.
    /// * `qty` - Calculated position quantity for the simulated trade.
    pub(crate) fn open_backtest_trade_from_intent(
        &mut self,
        intent: TradeIntent,
        qty: f64,
    ) {
        let is_long =
            intent.is_long;

        let signed_qty =
            if is_long {
                qty
            } else {
                -qty
            };

        let notional = intent.trade_amount_usd;

        let id =
            format!(
                "{}-{}-{}-BACKTEST",
                intent.strategy,
                intent.symbol,
                intent.ts_ms,
            );

        let cid =
            format!(
                "backtest-{}",
                id
            );

        let oid =
            format!(
                "backtest-{}",
                id
            );

        // Human-readable configured maximum trade duration.
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
        // CREATE BACKTEST TRADE
        // ============================================================

        // Backtest execution assumes an immediate fill at the historical
        // price contained in the TradeIntent.
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
                oid,
                cid,
                strategy: intent.strategy.clone(),
                // Trades timestamp
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
                // Backtests use historical event timestamps rather than
                // real runtime clocks for trade timing and duration logic.
                entry_time: None,
                entry_wall_time: None,
                // Only for simulation mode not backtest mode
                is_simulated: false,
                close_cid: None,
                close_oid: None,
            };

        // ============================================================
        // STORE TRADE
        // ============================================================

        self.trades.upsert(
            trade
        );

        // ============================================================
        // RECORD ENGINE STATE
        // ============================================================

        // Record which strategy currently owns this symbol.
        self.owner_by_symbol.insert(
            intent.symbol.clone(),
            intent.strategy.clone(),
        );

        // Record one open position for this symbol.
        *self.open_count_by_symbol
            .entry(intent.symbol.clone())
            .or_default() += 1;

        // Record the historical timestamp of the accepted trade.
        self.last_decision_ts.insert(
            intent.symbol.clone(),
            intent.ts_ms,
        );

        // ============================================================
        // BACKTEST OPEN LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE BACKTEST] ENTRY TRADE | \
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