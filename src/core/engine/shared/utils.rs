use crate::core::{
    config::Config,
    engine::{
        engine::Engine,
        state::ExecutionMode,
    },
};

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
