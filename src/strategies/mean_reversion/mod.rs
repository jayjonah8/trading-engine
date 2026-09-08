mod state;
mod strategy;
mod window;

mod backtest_strategy;
pub mod backtest_window;

// pub use state::MeanReversionStrategy;

pub use strategy::make_mean_reversion;

pub use backtest_strategy::
    make_mean_reversion_backtest;