#![allow(dead_code)]

use chrono::{DateTime, Utc};

use crate::core::{
    engine::engine::Engine,
    execution::Venue,
};


// ============================================================
// BACKTEST RESULT
// ============================================================

/// Immutable snapshot of a completed historical backtest.
///
/// Built directly from authoritative Engine state.
///
/// Data sources:
///
/// - Config: Execution venue and symbols.
/// - General: Account balances, realized PnL, trade counts.
/// - Metrics: Execution fees and future performance metrics.
/// - Trades: Remaining positions and unrealized PnL.
/// - Engine: Shared PnL and performance calculation methods.
///
/// BacktestResult is independent of EngineStatus.
///
/// TODO:
/// Add Sharpe ratio, maximum drawdown, profit factor,
/// win rate, expected value, and other performance metrics.
#[derive(Debug, Clone)]
pub struct BacktestResult {

    // ========================================================
    // CONFIGURATION
    // ========================================================

    /// Execution venue used during the backtest.
    pub execution_venue: Venue,

    /// Number of configured symbols.
    pub total_symbols: usize,


    // ========================================================
    // BACKTEST PERIOD
    // ========================================================

    /// Historical timestamp of the first processed market event.
    pub starting_date: DateTime<Utc>,

    /// Historical timestamp of the last processed market event.
    pub ending_date: DateTime<Utc>,

    /// Number of elapsed calendar days.
    pub total_trading_days: u64,


    // ========================================================
    // ACCOUNT PERFORMANCE
    // ========================================================

    /// Initial account balance.
    pub starting_balance: f64,

    /// Final cash balance based on realized PnL.
    ///
    /// Does not include unrealized PnL.
    pub ending_balance: f64,

    /// Final estimated account equity.
    ///
    /// Includes unrealized PnL from remaining open positions.
    pub ending_equity: f64,

    /// Total realized PnL after applicable execution fees.
    pub realized_pnl: f64,

    /// Gross unrealized PnL from remaining open positions.
    pub unrealized_pnl: f64,

    /// Net unrealized PnL after remaining entry fees
    /// and estimated exit fees.
    pub net_unrealized_pnl: f64,

    /// Total percentage return relative to starting balance.
    pub total_return_pct: f64,


    // ========================================================
    // TRADING ACTIVITY
    // ========================================================

    /// Number of fully closed trades.
    pub closed_trades: u64,

    /// Number of trades with remaining open quantity.
    pub open_trades: usize,

    /// Number of active trade lifecycles.
    pub active_trades: usize,


    // ========================================================
    // EXECUTION FEES
    // ========================================================

    /// Total confirmed entry execution fees.
    pub entry_fees: f64,

    /// Total confirmed exit execution fees.
    pub exit_fees: f64,

    /// Combined confirmed execution fees.
    pub total_fees: f64,
}



// ============================================================
// ENGINE BACKTEST RESULT IMPLEMENTATION
// ============================================================

impl Engine {

    // ========================================================
    // GET BACKTEST RESULT
    // ========================================================

    /// Builds the final backtest result directly from Engine.
    ///
    /// Must be called only after all historical events,
    /// execution commands, and execution fills have completed.
    ///
    /// Does not mutate engine state.
    ///
    /// Does not depend on EngineStatus.
    pub fn get_backtest_result(&self) -> BacktestResult {

        // ====================================================
        // ACCOUNT PERFORMANCE
        // ====================================================

        let starting_balance =
            self.general.starting_balance;

        // Backtest uses the normal realized PnL field.
        //
        // Simulation uses sim_realized_pnl instead.
        let realized_pnl =
            self.general.realized_pnl;

        // Reuse existing Engine PnL calculations.
        let unrealized_pnl =
            self.get_total_unrealized_pnl();

        let net_unrealized_pnl =
            self.get_total_net_unrealized_pnl();


        // ====================================================
        // ENDING BALANCE / EQUITY
        // ====================================================

        // Realized PnL is assumed to already include fees.
        //
        // Do not subtract Metrics.total_fees() again.
        let ending_balance =
            starting_balance + realized_pnl;

        // Gross mark-to-market equity.
        let ending_equity =
            ending_balance + unrealized_pnl;


        // ====================================================
        // TOTAL RETURN
        // ====================================================

        let total_return_pct =
            if starting_balance > 0.0 {
                (
                    (ending_equity - starting_balance)
                    / starting_balance
                ) * 100.0
            } else {
                0.0
            };


        // ====================================================
        // HISTORICAL TIME
        // ====================================================

        let starting_date =
            self.general.starting_date;

        let ending_date =
            self.get_current_engine_date();

        let total_trading_days =
            ending_date
                .signed_duration_since(starting_date)
                .num_days()
                .max(0) as u64;


        // ====================================================
        // FINAL RESULT
        // ====================================================

        BacktestResult {

            // Configuration.
            execution_venue: self.cfg.execution_venue,
            total_symbols: self.cfg.symbols.len(),

            // Historical period.
            starting_date,
            ending_date,
            total_trading_days,

            // Account performance.
            starting_balance,
            ending_balance,
            ending_equity,

            realized_pnl,
            unrealized_pnl,
            net_unrealized_pnl,
            total_return_pct,

            // Trading activity.
            closed_trades: self.general.live_closed_trades,
            open_trades:
                self.state.trades.count_with_open_quantity(),
            active_trades:
                self.state.trades.count_active(),

            // Execution fees.
            entry_fees: self.metrics.entry_fees,
            exit_fees: self.metrics.exit_fees,
            total_fees: self.metrics.total_fees(),
        }
    }
}