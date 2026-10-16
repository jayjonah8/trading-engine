use crate::core::engine::engine::Engine;
use crate::core::engine::state::ExecutionMode; 


// ============================================================
// ENGINE EQUITY SAMPLING
// ============================================================

impl Engine {
    /// Records account equity at one-minute intervals across all
    /// execution modes: Live, Paper, Simulation, and Backtest.
    ///
    /// Uses normalized market event timestamps (Unix milliseconds)
    /// supplied by the Engine, regardless of execution mode.
    ///
    /// TODO:
    /// This initial implementation samples equity when the first
    /// market event of a new minute arrives, rather than at the
    /// exact previous minute boundary.
    ///
    /// Missing market events may cause intermediate minutes to
    /// be skipped in both live trading and historical backtesting.
    ///
    /// Future improvements should implement fixed interval
    /// sampling and handle missing intervals consistently
    /// across all execution modes.
    pub fn sample_equity(&mut self, ts_ms: i64) {
        const SAMPLE_INTERVAL_MS: i64 = 60_000;

        // Determine the current minute.
        let current_minute =
            ts_ms.div_euclid(SAMPLE_INTERVAL_MS);

        // Initialize equity metrics.
        if self.metrics.last_equity_sample_minute.is_none() {
            self.metrics.record_equity(
                self.general.starting_balance
            );

            self.metrics.last_equity_sample_minute =
                Some(current_minute);

            return;
        }

        let last_minute =
            self.metrics.last_equity_sample_minute.unwrap();

        // Only sample when the minute advances.
        if current_minute <= last_minute {
            return;
        }

        // Select the correct realized PnL for the execution mode.
        let realized_pnl = match self.execution_mode {
            ExecutionMode::Simulation => {
                self.general.sim_realized_pnl
            }

            ExecutionMode::Live
            | ExecutionMode::Paper
            | ExecutionMode::Backtest => {
                self.general.realized_pnl
            }
        };

        // Calculate account equity.
        let equity =
            self.general.starting_balance
                + realized_pnl
                + self.get_total_net_unrealized_pnl();

        // Record the equity sample.
        self.metrics.record_equity(equity);

        self.metrics.last_equity_sample_minute =
            Some(current_minute);
    }
}