#![allow(dead_code)]

mod strategy;
mod backtest_strategy;

pub use strategy::IndicatorStrategy;
pub use strategy::make_indicator_strategy;
pub use backtest_strategy::make_indicator_backtest;