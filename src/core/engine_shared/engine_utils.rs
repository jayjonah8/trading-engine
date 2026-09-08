use crate::core::{
    config::Config,
    engine_state::ExecutionMode,
};

use crate::core::engine_state::Engine;


// ============================================================
// Engine State Utils 
// ============================================================

impl Engine {
    /// Returns the current number of open trades tracked by the engine.
    pub fn get_open_trade_count(
        &self,
    ) -> usize {
        self.trades.count_open()
    }

    // Number of positions the bot has "booked":
    //   - Open trades already on the book
    //   - Pending open orders that may become open trades
    //   - Pending close orders are NOT added because their trade is already booked
    //   - Used to prevent exceeding max_gross_positions
    pub fn booked_positions(&self) -> usize {
        let live_open = self.trades.count_open();
        let pending_open = self.pending_opens.len();

        live_open + pending_open
    }
}


// ============================================================
// General Engine Helper Functions
// ============================================================

/// Verifies that the engine startup configuration contains
/// a valid and non-conflicting execution mode.
///
/// Panics if the engine configuration is invalid.
///
/// Backtest and simulation modes cannot be enabled at the same time.
pub fn verify_engine_config(cfg: &Config) {
    if cfg.enable_backtest_mode
        && cfg.enable_simulation_mode
    {
        panic!(
            "Invalid engine config: backtest mode and simulation mode \
             cannot be enabled at the same time."
        );
    }
}


/// Determines the engine execution mode from the validated startup config.
pub fn get_execution_mode(
    cfg: &Config,
) -> ExecutionMode {
    if cfg.enable_backtest_mode {
        ExecutionMode::Backtest
    } else if cfg.enable_simulation_mode {
        ExecutionMode::Simulation
    } else {
        ExecutionMode::Live
    }
}

