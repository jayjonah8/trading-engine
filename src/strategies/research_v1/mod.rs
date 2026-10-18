mod state;
mod backtest_state;
mod strategy;
mod backtest_strategy;

// Strategy struct.
pub use state::ResearchV1Strategy;

// Live strategy factory.
pub use strategy::make_research_v1;

// Backtest strategy factory.
pub use backtest_strategy::{
    make_research_v1_backtest,
};