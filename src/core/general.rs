#![allow(dead_code)]

use chrono::{DateTime, Utc};

// ============================================================
// GENERAL ENGINE STATE
// ============================================================

#[derive(Debug, Default, Clone)]
pub struct General {
    /// Total realized PnL produced by live broker-executed trades.
    pub realized_pnl: f64,

    /// Total realized PnL produced by simulated trades.
    pub sim_realized_pnl: f64,

    /// Account balance captured when the engine starts.
    ///
    /// This remains the fixed starting balance used as the baseline
    /// for performance and account-level calculations.
    pub starting_balance: f64,

    /// Date and time when the engine session started.
    ///
    /// Backtests should use the historical backtest start time rather
    /// than the current wall-clock time.
    pub starting_date: DateTime<Utc>,

    /// Total number of completed live broker-executed trades.
    ///
    /// Incremented once when a live Trade becomes fully closed.
    pub live_closed_trades: u64,

    /// Total number of completed simulated trades.
    ///
    /// Incremented once when a simulated Trade becomes fully closed.
    pub sim_closed_trades: u64,
}