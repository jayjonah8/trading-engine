#![allow(dead_code)]

use crate::core::{
    engine::state::ExecutionMode,
    execution::Venue,
};

use chrono::{DateTime, Utc};

use crate::core::{
    engine::{
        engine::Engine,
    },
};


// Usage:
//   Inside impl Engine:  let status = self.get_status();
//   Outside impl Engine: let status = engine.get_status();


// ============================================================
// WARNING: STATUS PERFORMANCE
// ============================================================
//
// Do NOT include EngineStatus in EngineView or publish_view().
// EngineView updates after every event, including high-frequency
// price ticks, causing unnecessary status recalculations.
//
// get_status() always returns a fresh snapshot when called.
//
// Strategies access engine state through EngineView, not directly
// through Engine. If strategies need status information, expose
// only the specific metrics they require through EngineView.
//
// TODO:
// Add periodic status polling through the engine event loop
// without impacting high-frequency trading performance.


// ============================================================
// ENGINE STATUS
// ============================================================

/// Read-only snapshot of the trading engine's current status.
///
/// Intended for:
/// - Slack notifications
/// - Discord notifications
/// - Status polling
/// - Monitoring dashboards
/// - Console reporting
///
/// All values represent the current execution mode.
#[derive(Debug, Clone)]
pub struct EngineStatus {
   
    /// Current execution mode.
    pub execution_mode: ExecutionMode,

    /// Current execution venue.
    pub execution_venue: Venue,

    /// Number of configured trading symbols.
    pub total_symbols: usize,

    /// Number of active Trade lifecycles.
    ///
    /// Includes Opening, Open, and Closing trades.
    pub active_trades: usize,

    /// Number of Trades with actual remaining exposure.
    pub open_trades: usize,

    /// Number of fully closed Trades in the current mode.
    pub closed_trades: u64,

    /// Total unrealized PnL across currently open Trades.
    ///
    /// Gross unrealized PnL based on latest known prices.
    pub total_unrealized_pnl: f64,

    /// Total net unrealized PnL across currently open Trades.
    ///
    /// Gross unrealized PnL minus:
    /// - Actual entry fees allocated to remaining open quantity.
    /// - Estimated fees required to close remaining open quantity.
    pub total_net_unrealized_pnl: f64,

    /// Total realized PnL for the current execution mode.
    ///
    /// Simulation:
    ///     General.sim_realized_pnl
    ///
    /// Live / Paper / Backtest:
    ///     General.realized_pnl
    ///
    /// These values are NEVER added together.
    pub total_realized_pnl: f64,

    
    /// Total accumulated execution fees.
    pub total_execution_fees: f64,

    /// Account balance captured when the engine started.
    pub starting_balance: f64,

    /// Number of elapsed calendar days since the engine started.
    ///
    /// This is session duration, not exchange trading sessions.
    pub total_trading_days: u64,
}



// ============================================================
// ENGINE STATUS IMPLEMENTATION
// ============================================================

impl Engine {
    // ============================================================
    // GET STATUS
    // ============================================================

    /// Builds a read-only snapshot of the current engine status.
    ///
    /// Does not mutate trading state.
    ///
    /// Intended for periodic monitoring and final backtest reporting.
    pub fn get_status(&self) -> EngineStatus {
        EngineStatus {
            // ====================================================
            // EXECUTION CONFIGURATION
            // ====================================================

            execution_mode: self.cfg.execution_mode,
            execution_venue: self.cfg.execution_venue,
            total_symbols: self.cfg.symbols.len(),

            // ====================================================
            // TRADE COUNTS
            // ====================================================

            active_trades: self.state.trades.count_active(),
            open_trades: self.state.trades.count_with_open_quantity(),
            closed_trades: self.get_closed_trade_count(),

            // ====================================================
            // PROFIT AND LOSS
            // ====================================================

            total_unrealized_pnl: self.get_total_unrealized_pnl(),
            total_net_unrealized_pnl: self.get_total_net_unrealized_pnl(),
            total_realized_pnl: self.get_total_realized_pnl(),

            // ====================================================
            // FEES
            // ====================================================

            total_execution_fees: self.metrics.total_fees(),

            // ====================================================
            // ACCOUNT / PERFORMANCE
            // ====================================================

            starting_balance: self.general.starting_balance,
            total_trading_days: self.get_total_trading_days(),
        }
    }


    // ============================================================
    // CLOSED TRADE COUNT
    // ============================================================

    /// Returns the total number of closed trades for the
    /// current execution mode.
    ///
    /// Simulation uses its own counter.
    ///
    /// Live, Paper, and Backtest use the normal counter.
    pub fn get_closed_trade_count(&self) -> u64 {
        match self.cfg.execution_mode {
            ExecutionMode::Simulation => {
                self.general.sim_closed_trades
            }

            ExecutionMode::Live
            | ExecutionMode::Paper
            | ExecutionMode::Backtest => {
                self.general.live_closed_trades
            }
        }
    }


    // ============================================================
    // TOTAL UNREALIZED PNL
    // ============================================================

    /// Calculates gross unrealized PnL across all trades
    /// with remaining open quantity.
    ///
    /// Uses each trade's individual unrealized_pnl() method.
    ///
    /// Trades without a known market price contribute zero.
    ///
    /// Does not deduct execution fees or estimated closing costs.
    pub fn get_total_unrealized_pnl(&self) -> f64 {
        self.state
            .trades
            .iter_with_open_quantity()
            .filter_map(|trade| {
                self.last_px
                    .get(&trade.symbol)
                    .map(|last| {
                        trade.unrealized_pnl(last.px)
                    })
            })
            .sum()
    }


    // ============================================================
    // TOTAL NET UNREALIZED PNL
    // ============================================================

    /// Calculates net unrealized PnL across all Trades
    /// with remaining open quantity.
    ///
    /// Net Unrealized PnL:
    ///
    ///     Gross Unrealized PnL
    ///     - Remaining Entry Fees
    ///     - Estimated Exit Fees
    ///
    /// Estimated exit fees are assumed equal to the remaining
    /// entry fees. Actual exit fees are recorded when fills occur.
    pub fn get_total_net_unrealized_pnl(&self) -> f64 {
        self.state
            .trades
            .iter_with_open_quantity()
            .filter_map(|trade| {
                self.last_px
                    .get(&trade.symbol)
                    .map(|last| {
                        // Gross unrealized PnL.
                        let gross_pnl = trade.unrealized_pnl(last.px);

                        // Actual entry fees attributable to remaining quantity.
                        let remaining_entry_fees =
                            (trade.entry_fees - trade.allocated_entry_fees)
                                .max(0.0);

                        // Estimate exit fees equal to remaining entry fees.
                        let estimated_exit_fees = remaining_entry_fees;

                        // Net unrealized PnL after estimated round-trip fees.
                        gross_pnl
                            - remaining_entry_fees
                            - estimated_exit_fees
                    })
            })
            .sum()
    }


    // ============================================================
    // TOTAL REALIZED PNL
    // ============================================================

    /// Returns realized PnL for the current execution mode.
    ///
    /// Simulation uses sim_realized_pnl.
    ///
    /// Live, Paper, and Backtest use realized_pnl.
    pub fn get_total_realized_pnl(&self) -> f64 {
        match self.cfg.execution_mode {
            ExecutionMode::Simulation => {
                self.general.sim_realized_pnl
            }

            ExecutionMode::Live
            | ExecutionMode::Paper
            | ExecutionMode::Backtest => {
                self.general.realized_pnl
            }
        }
    }


    // ============================================================
    // CURRENT ENGINE DATE
    // ============================================================

    /// Returns the current time according to the execution mode.
    ///
    /// Live / Paper / Simulation:
    ///     Current UTC wall-clock time.
    ///
    /// Backtest:
    ///     Latest historical market timestamp.
    ///
    /// TODO:
    /// Replace historical timestamp lookup with a dedicated
    /// backtest replay clock when available.
    pub fn get_current_engine_date(&self) -> DateTime<Utc> {
        match self.cfg.execution_mode {
            ExecutionMode::Backtest => {
                self.last_px
                    .values()
                    .map(|last| last.ts_ms)
                    .max()
                    .and_then(DateTime::<Utc>::from_timestamp_millis)
                    .unwrap_or(self.general.starting_date)
            }

            ExecutionMode::Live
            | ExecutionMode::Paper
            | ExecutionMode::Simulation => {
                Utc::now()
            }
        }
    }


    // ============================================================
    // TOTAL TRADING DAYS
    // ============================================================

    /// Returns elapsed calendar days since engine start.
    ///
    /// Backtest uses historical replay time.
    ///
    /// Live / Paper / Simulation use wall-clock time.
    ///
    /// NOTE:
    /// Includes weekends and holidays.
    pub fn get_total_trading_days(&self) -> u64 {
        self.get_current_engine_date()
            .signed_duration_since(self.general.starting_date)
            .num_days()
            .max(0) as u64
    }
}