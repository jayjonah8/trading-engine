#![allow(dead_code)]

use std::collections::HashSet;

use crate::{
    core::trades::Trades,
    shared::print_log,
    toolbox::MarketRegime,
};

use super::{
    config::MultiSyncConfig,

    state::{
        MultiSyncState,
        MultiSyncWindow,
        MultiSyncWindowKind,
        MultiSyncProfitProtectionState,
        MultiSyncDrawdownProtectionState,
    },

    timer,
};


// WARNING:
// Multi-Sync relies on the engine retaining completed trade records
// until their realized PnL has been processed and recorded.
// Premature "Trade" removal or pruning from engine state may cause missed trades,
// incorrect PnL accounting, and protection failures.


// ============================================================
// MULTI-SYNC TOOLBOX
// ============================================================
//
// PURPOSE:
// Implements the Multi-Sync Profit Protection and Drawdown
// Protection systems.
//
// PRIMARY REFERENCE:
// Combined Multi-Sync Continuous V2 simulation.
//
// CORE BEHAVIOR:
// - Accumulates strategy-specific realized PnL.
// - Polls completed trades from authoritative EngineView state.
// - Processes completed trades exactly once per MultiSync instance.
// - Evaluates profit and drawdown thresholds.
// - Activates temporary regime reversal windows.
// - Restores the base regime at scheduled expiration.
// - Completes accounting on the first completed trade
//   at or after the scheduled expiration.
// - Advances protection stages.
// - Resets the opposite protection stage.
// - Resets the shared realized PnL anchor.
// - Prevents window-entered trades from retriggering.
//
// Each strategy owns an independent MultiSync instance.
//
// Multi-Sync runs continuously without daily resets.
//
// No additional engine events or synchronization channels
// are required.
//
// ============================================================


pub struct MultiSync {
    pub config: MultiSyncConfig,

    pub state: MultiSyncState,

    pub debug_mode: bool,

    /// Normal strategy regime outside Multi-Sync windows.
    base_regime: MarketRegime,
}


// ============================================================
// MULTI-SYNC IMPLEMENTATION
// ============================================================

impl MultiSync {

    // ========================================================
    // INITIALIZATION
    // ========================================================

    pub fn new(
        config: MultiSyncConfig,
        base_regime: MarketRegime,
    ) -> Result<Self, &'static str> {
        config.validate()?;

        let state = MultiSyncState {
            running_realized_pnl: 0.0,

            anchor_realized_pnl: 0.0,

            active_window: None,

            historical_windows: Vec::new(),

            // ====================================================
            // PROCESSED CLOSED TRADES
            // ====================================================

            processed_trade_ids: HashSet::new(),

            // ====================================================
            // PROFIT PROTECTION
            // ====================================================

            profit_protection: MultiSyncProfitProtectionState {
                enabled: config.enabled
                    && config.profit_protection.enabled,

                settings: config
                    .profit_protection
                    .default_settings
                    .clone(),

                current_idx: 0,

                window_triggers: 0,

                completed_cycles: 0,
            },

            // ====================================================
            // DRAWDOWN PROTECTION
            // ====================================================

            drawdown_protection: MultiSyncDrawdownProtectionState {
                enabled: config.enabled
                    && config.drawdown_protection.enabled,

                settings: config
                    .drawdown_protection
                    .default_settings
                    .clone(),

                current_idx: 0,

                window_triggers: 0,

                completed_cycles: 0,
            },
        };

        let debug_mode = config.debug_mode;

        Ok(Self {
            config,
            state,
            debug_mode,
            base_regime,
        })
    }


    // ========================================================
    // FEATURE STATUS
    // ========================================================

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }


    // ========================================================
    // WINDOW STATUS
    // ========================================================

    /// Returns whether the regime reversal is active
    /// at the supplied timestamp.
    pub fn is_window_active(
        &self,
        ts_ms: i64,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }

        self.state
            .active_window
            .as_ref()
            .map_or(false, |window| {
                timer::is_regime_window_active(
                    window,
                    ts_ms,
                )
            })
    }


    // ========================================================
    // PENDING ACCOUNTING WINDOW
    // ========================================================

    /// Returns whether a window is still awaiting
    /// accounting completion.
    pub fn has_pending_window(&self) -> bool {
        self.state.active_window.is_some()
    }


    // ========================================================
    // HISTORICAL WINDOW CLASSIFICATION
    // ========================================================

    /// Determines whether a trade opened during any
    /// previously triggered Multi-Sync window.
    ///
    /// V2 FIX:
    /// These trades contribute realized PnL but cannot
    /// directly trigger additional windows.
    pub fn opened_during_window(
        &self,
        entry_ts_ms: i64,
    ) -> bool {
        self.state
            .historical_windows
            .iter()
            .any(|window| {
                window.includes_entry(entry_ts_ms)
            })
    }


    // ========================================================
    // REALIZED PNL PRESSURE
    // ========================================================

    /// Calculates realized PnL pressure since the anchor.
    ///
    /// Mirrors V2:
    ///
    /// (running_abs - anchor_abs) / margin * 100
    pub fn current_pnl_pressure_pct(
        &self,
        reference_margin_usd: f64,
    ) -> Option<f64> {
        if !reference_margin_usd.is_finite()
            || reference_margin_usd <= 0.0
        {
            return None;
        }

        let pnl_since_anchor =
            self.state.running_realized_pnl
                - self.state.anchor_realized_pnl;

        Some(
            (pnl_since_anchor / reference_margin_usd) * 100.0
        )
    }


    // ========================================================
    // REGIME REVERSAL
    // ========================================================

    /// Returns the opposite of the supplied market regime.
    fn opposite_regime(
        regime: MarketRegime,
    ) -> MarketRegime {
        match regime {
            MarketRegime::MeanReversion => MarketRegime::Trending,

            MarketRegime::Trending => MarketRegime::MeanReversion,
        }
    }


    // ========================================================
    // APPLY MARKET REGIME
    // ========================================================

    /// Applies the correct regime for the supplied timestamp.
    ///
    /// During an active window:
    ///     Uses the opposite of the base regime.
    ///
    /// Outside an active window:
    ///     Restores the base regime.
    fn apply_regime(
        &self,
        ts_ms: i64,
        market_regime: &mut MarketRegime,
    ) {
        *market_regime = if self.is_window_active(ts_ms) {
            Self::opposite_regime(self.base_regime)
        } else {
            self.base_regime
        };
    }


    // ========================================================
    // TIMER TICK
    // ========================================================

    /// Updates the strategy's market regime using the
    /// supplied event timestamp.
    ///
    /// Must be called before evaluating new trade entries.
    ///
    /// Does not complete accounting or advance stages.
    pub fn on_timer_tick(
        &mut self,
        ts_ms: i64,
        market_regime: &mut MarketRegime,
    ) {
        let previous_regime = *market_regime;

        self.apply_regime(
            ts_ms,
            market_regime,
        );

        // ====================================================
        // DEBUG LOGGING
        // ====================================================

        if self.debug_mode && previous_regime != *market_regime {
            print_log(
                true,
                false,
                format_args!(
                    "[MULTI-SYNC] Regime changed | \
                     {:?} -> {:?} | timestamp={}",
                    previous_regime,
                    market_regime,
                    ts_ms,
                ),
            );
        }
    }


    // ========================================================
    // PROCESS CLOSED TRADES
    // ========================================================

    /// Polls authoritative engine trade state and processes
    /// newly completed trades belonging to one strategy.
    ///
    /// The engine remains authoritative for:
    /// - Trade lifecycle.
    /// - Execution prices.
    /// - Entry and exit fees.
    /// - Trade timestamps.
    ///
    /// Multi-Sync only consumes completed trade results.
    ///
    /// BEHAVIOR:
    /// - Filters completed trades by strategy.
    /// - Skips previously processed TradeIds.
    /// - Skips trades closed after the current event timestamp.
    /// - Processes trades in deterministic chronological order.
    /// - Calculates final net realized PnL.
    /// - Updates Multi-Sync protection state.
    /// - Updates the effective market regime.
    ///
    /// No additional engine events or synchronization channels
    /// are required.
    ///
    /// Returns the number of newly processed completed trades.
    pub fn process_closed_trades(
        &mut self,
        trades: &Trades,
        strategy_id: &str,
        reference_margin_usd: f64,
        current_ts_ms: i64,
        market_regime: &mut MarketRegime,
    ) -> usize {

        // ====================================================
        // DISABLED
        // ====================================================

        if !self.config.enabled {
            self.on_timer_tick(
                current_ts_ms,
                market_regime,
            );

            return 0;
        }

        // ====================================================
        // VALIDATE REFERENCE MARGIN
        // ====================================================

        if !reference_margin_usd.is_finite()
            || reference_margin_usd <= 0.0
        {
            print_log(
                self.debug_mode,
                true,
                format_args!(
                    "[MULTI-SYNC] Invalid reference margin | margin={}",
                    reference_margin_usd,
                ),
            );

            self.on_timer_tick(
                current_ts_ms,
                market_regime,
            );

            return 0;
        }

        // ====================================================
        // COLLECT NEW COMPLETED TRADES
        // ====================================================

        let mut completed_trades: Vec<_> = trades
            .closed_trades_for_strategy(strategy_id)
            .filter(|trade| {
                !self.state
                    .processed_trade_ids
                    .contains(&trade.id)
            })
            .filter(|trade| {
                trade.closed_ts_ms
                    .map_or(false, |ts| ts <= current_ts_ms)
            })
            .collect();

        // ====================================================
        // DETERMINISTIC CHRONOLOGICAL ORDER
        // ====================================================

        // Sort by closure timestamp.
        //
        // When two trades close at the same timestamp,
        // use the string representation of TradeId
        // as a deterministic tie-breaker.
        //
        // This does not require TradeId to implement Ord.

        completed_trades.sort_by(|a, b| {
            a.closed_ts_ms
                .cmp(&b.closed_ts_ms)
                .then_with(|| {
                    a.id.to_string()
                        .cmp(&b.id.to_string())
                })
        });

        // ====================================================
        // PROCESS COMPLETED TRADES
        // ====================================================

        let mut processed_count = 0;

        for trade in completed_trades {

            // ====================================================
            // VALIDATE TIMESTAMPS
            // ====================================================

            let Some(entry_ts_ms) =
                trade.opened_ts_ms
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[MULTI-SYNC] Closed trade missing entry timestamp | trade_id={}",
                        trade.id,
                    ),
                );

                continue;
            };

            let Some(exit_ts_ms) =
                trade.closed_ts_ms
            else {
                continue;
            };

            if entry_ts_ms > exit_ts_ms {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[MULTI-SYNC] Invalid trade timestamps | trade_id={} | entry={} | exit={}",
                        trade.id,
                        entry_ts_ms,
                        exit_ts_ms,
                    ),
                );

                continue;
            }

            // ====================================================
            // VALIDATE EXECUTION PRICES
            // ====================================================

            let Some(entry_px) =
                trade.avg_entry_px
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[MULTI-SYNC] Closed trade missing entry price | trade_id={}",
                        trade.id,
                    ),
                );

                continue;
            };

            let Some(exit_px) =
                trade.avg_exit_px
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[MULTI-SYNC] Closed trade missing exit price | trade_id={}",
                        trade.id,
                    ),
                );

                continue;
            };

            if !entry_px.is_finite()
                || !exit_px.is_finite()
                || entry_px <= 0.0
                || exit_px <= 0.0
            {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[MULTI-SYNC] Invalid execution prices | trade_id={}",
                        trade.id,
                    ),
                );

                continue;
            }

            // ====================================================
            // CALCULATE GROSS REALIZED PNL
            // ====================================================

            let gross_realized_pnl =
                match trade.direction {
                    crate::core::trades::Direction::Long => {
                        (exit_px - entry_px)
                            * trade.exited_quantity
                    }

                    crate::core::trades::Direction::Short => {
                        (entry_px - exit_px)
                            * trade.exited_quantity
                    }
                };

            // ====================================================
            // CALCULATE NET REALIZED PNL
            // ====================================================

            // Mirrors the completed-trade calculation in
            // core/engine/exec_handler/fill.rs.
            //
            // Do not subtract allocated_entry_fees separately.
            // They are already included in total entry_fees.

            let net_realized_pnl =
                gross_realized_pnl
                    - trade.entry_fees
                    - trade.exit_fees;

            if !net_realized_pnl.is_finite() {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[MULTI-SYNC] Non-finite realized PnL | trade_id={}",
                        trade.id,
                    ),
                );

                continue;
            }

            // ====================================================
            // UPDATE MULTI-SYNC
            // ====================================================

            match self.on_trade_closed(
                net_realized_pnl,
                reference_margin_usd,
                entry_ts_ms,
                exit_ts_ms,
                market_regime,
            ) {
                Ok(()) => {

                    // Mark the trade as processed only after
                    // Multi-Sync successfully accounts for it.

                    self.state
                        .processed_trade_ids
                        .insert(trade.id);

                    processed_count += 1;
                }

                Err(err) => {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[MULTI-SYNC] Failed to process closed trade | trade_id={} | error={}",
                            trade.id,
                            err,
                        ),
                    );
                }
            }
        }

        // ====================================================
        // APPLY CURRENT MARKET REGIME
        // ====================================================

        // The final regime must reflect the current market
        // event timestamp, not the timestamp of the last
        // completed trade.

        self.on_timer_tick(
            current_ts_ms,
            market_regime,
        );

        processed_count
    }


    // ========================================================
    // TRADE CLOSED
    // ========================================================

    /// Processes a completed trade belonging to this strategy.
    ///
    /// IMPORTANT:
    /// - Call exactly once per completed trade.
    /// - Use the trade's actual net realized PnL.
    /// - Use a stable strategy-specific reference margin.
    /// - Process completed trades in timestamp order.
    ///
    /// This method does not query global engine PnL.
    pub fn on_trade_closed(
        &mut self,
        realized_pnl: f64,
        reference_margin_usd: f64,
        entry_ts_ms: i64,
        exit_ts_ms: i64,
        market_regime: &mut MarketRegime,
    ) -> Result<(), &'static str> {

        // ====================================================
        // VALIDATION
        // ====================================================

        if !realized_pnl.is_finite() {
            return Err("Multi-Sync realized PnL must be finite");
        }

        if !reference_margin_usd.is_finite()
            || reference_margin_usd <= 0.0
        {
            return Err("Multi-Sync reference margin must be positive");
        }

        if entry_ts_ms > exit_ts_ms {
            return Err("Trade entry timestamp exceeds exit timestamp");
        }

        if !self.config.enabled {
            *market_regime = self.base_regime;
            return Ok(());
        }

        // ====================================================
        // HISTORICAL WINDOW CLASSIFICATION
        // ====================================================

        // A trade opened during a Multi-Sync window
        // cannot directly trigger another window.

        let opened_inside_window =
            self.opened_during_window(entry_ts_ms);

        // ====================================================
        // UPDATE CUMULATIVE REALIZED PNL
        // ====================================================

        // V2 accumulates PnL before checking expiration
        // or evaluating protection thresholds.

        self.state.running_realized_pnl += realized_pnl;

        // ====================================================
        // ACCOUNTING WINDOW COMPLETION
        // ====================================================

        // If the current window has reached its deadline,
        // this closing trade contributes PnL BEFORE the
        // shared anchor is reset.
        //
        // The same trade cannot trigger another window.

        if let Some(window) = self.state.active_window.as_ref() {
            if timer::should_complete_accounting(
                window,
                exit_ts_ms,
            ) {
                self.complete_window();

                self.apply_regime(
                    exit_ts_ms,
                    market_regime,
                );

                return Ok(());
            }
        }

        // ====================================================
        // ACTIVE OR PENDING WINDOW
        // ====================================================

        // While a window is pending accounting completion,
        // additional triggers are blocked.

        if self.has_pending_window() {
            self.apply_regime(
                exit_ts_ms,
                market_regime,
            );

            return Ok(());
        }

        // ====================================================
        // V2 RETRIGGER PREVENTION
        // ====================================================

        // Window-entered trades contribute realized PnL,
        // but cannot directly trigger another window.

        if opened_inside_window {
            self.apply_regime(
                exit_ts_ms,
                market_regime,
            );

            return Ok(());
        }

        // ====================================================
        // CALCULATE PNL PRESSURE
        // ====================================================

        let pnl_pct = self
            .current_pnl_pressure_pct(reference_margin_usd)
            .ok_or("Invalid Multi-Sync reference margin")?;

        // ====================================================
        // PROFIT PROTECTION
        // ====================================================

        if self.state.profit_protection.should_trigger(pnl_pct) {
            self.start_window(
                MultiSyncWindowKind::ProfitProtection,
                exit_ts_ms,
                pnl_pct,
            )?;

            self.apply_regime(
                exit_ts_ms,
                market_regime,
            );

            return Ok(());
        }

        // ====================================================
        // DRAWDOWN PROTECTION
        // ====================================================

        if self.state.drawdown_protection.should_trigger(pnl_pct) {
            self.start_window(
                MultiSyncWindowKind::DrawdownProtection,
                exit_ts_ms,
                pnl_pct,
            )?;

            self.apply_regime(
                exit_ts_ms,
                market_regime,
            );

            return Ok(());
        }

        // ====================================================
        // NO WINDOW TRIGGERED
        // ====================================================

        self.apply_regime(
            exit_ts_ms,
            market_regime,
        );

        Ok(())
    }


    // ========================================================
    // START WINDOW
    // ========================================================

    /// Activates a Profit Protection or Drawdown Protection
    /// window using the current stage configuration.
    fn start_window(
        &mut self,
        kind: MultiSyncWindowKind,
        ts_ms: i64,
        pnl_pct: f64,
    ) -> Result<(), &'static str> {

        // ====================================================
        // CURRENT WINDOW SETTING
        // ====================================================

        let window_ms = match kind {
            MultiSyncWindowKind::ProfitProtection => {
                self.state
                    .profit_protection
                    .current_setting()
                    .ok_or("Missing Profit Protection setting")?
                    .window_ms
            }

            MultiSyncWindowKind::DrawdownProtection => {
                self.state
                    .drawdown_protection
                    .current_setting()
                    .ok_or("Missing Drawdown Protection setting")?
                    .window_ms
            }
        };

        // ====================================================
        // CALCULATE WINDOW EXPIRATION
        // ====================================================

        let end_ts_ms = timer::calculate_window_end(
            ts_ms,
            window_ms,
        )?;

        // ====================================================
        // CREATE WINDOW
        // ====================================================

        let window = MultiSyncWindow {
            kind,
            start_ts_ms: ts_ms,
            end_ts_ms,
        };

        // ====================================================
        // UPDATE TRIGGER COUNTERS
        // ====================================================

        match kind {
            MultiSyncWindowKind::ProfitProtection => {
                self.state.profit_protection.window_triggers += 1;
            }

            MultiSyncWindowKind::DrawdownProtection => {
                self.state.drawdown_protection.window_triggers += 1;
            }
        }

        // ====================================================
        // STORE WINDOW
        // ====================================================

        self.state.historical_windows.push(window.clone());

        self.state.active_window = Some(window);

        // TODO(memory):
        // Prune historical windows when no open trades
        // can reference them, or store the window-entry
        // classification directly on each trade.

        // ====================================================
        // DEBUG LOGGING
        // ====================================================

        if self.debug_mode {
            print_log(
                true,
                false,
                format_args!(
                    "[MULTI-SYNC] {:?} triggered | \
                     pnl={:+.4}% | start={} | end={:?}",
                    kind,
                    pnl_pct,
                    ts_ms,
                    end_ts_ms,
                ),
            );
        }

        Ok(())
    }


    // ========================================================
    // COMPLETE WINDOW
    // ========================================================

    /// Completes the accounting lifecycle of an expired
    /// Multi-Sync window.
    ///
    /// The closing trade's PnL has already been accumulated
    /// before this method is called.
    fn complete_window(&mut self) {
        let Some(window) = self.state.active_window.take() else {
            return;
        };

        // ====================================================
        // STAGE PROGRESSION
        // ====================================================

        match window.kind {
            MultiSyncWindowKind::ProfitProtection => {
                self.state.profit_protection.advance_stage();

                self.state.drawdown_protection.reset_stage();
            }

            MultiSyncWindowKind::DrawdownProtection => {
                self.state.drawdown_protection.advance_stage();

                self.state.profit_protection.reset_stage();
            }
        }

        // ====================================================
        // RESET SHARED PNL ANCHOR
        // ====================================================

        // The anchor resets to cumulative realized PnL
        // INCLUDING the trade that completed the window.

        self.state.anchor_realized_pnl =
            self.state.running_realized_pnl;

        // ====================================================
        // DEBUG LOGGING
        // ====================================================

        if self.debug_mode {
            print_log(
                true,
                false,
                format_args!(
                    "[MULTI-SYNC] {:?} accounting completed | \
                     anchor={:+.4}",
                    window.kind,
                    self.state.anchor_realized_pnl,
                ),
            );
        }
    }
}