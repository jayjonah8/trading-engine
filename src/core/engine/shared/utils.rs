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
    // Todo
}


