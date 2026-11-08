pub mod state;
pub mod strategy;

pub mod backtest_state;
pub mod backtest_strategy;

pub use strategy::make_research_v4;
pub use backtest_strategy::make_research_v4_backtest;