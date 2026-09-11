mod state;
mod backtest_state;
mod strategy;
mod window;
mod backtest_strategy;


pub mod backtest_window;
pub use state::BaseStrategy;
pub use backtest_strategy::make_base_backtest;
pub mod strategy_session;
pub mod strategy_utils;