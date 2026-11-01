#![allow(dead_code)]

use std::time::Duration;

use crate::{
    core::{
        config::Config,
        engine::state::ExecutionMode,
        execution::Venue,
    },
    data::{
        historical::provider::HistoricalDataProvider,
        live::provider::LiveMarketDataProvider,
    },
};

/// Returns the default configuration used by integration/unit tests.
///
/// Tests should start from this configuration and override only the fields
/// relevant to the behavior being tested. When Config gains a new field,
/// only this default test configuration normally needs to be updated.
pub fn default_test_config() -> Config {
    Config {
        execution_mode: ExecutionMode::Live,
        execution_venue: Venue::Alpaca,
        live_market_data_provider: LiveMarketDataProvider::Alpaca,
        historical_data_provider: HistoricalDataProvider::Alpaca,
        symbols: vec!["AAPL".to_string(), "MSFT".to_string(), "NVDA".to_string()],
        backtest_start: "2026-10-15T10:00:00Z".to_string(),
        backtest_end: "2026-10-15T11:00:00Z".to_string(),
        backtest_min_trade_interval: Duration::from_millis(300),
        max_active_trades: Some(20),
        max_active_assets: Some(20),
        max_active_trades_per_symbol: Some(20),
        max_active_trades_per_strategy: Some(20),
        max_order_notional: Some(150_000.0),
        max_gross_notional: Some(150_000.0),
        max_symbol_notional: Some(150_000.0),
        custom_starting_balance: 100_000.0,
        close_trades_on_startup: false,
        enable_discord_notifications: false,
        enable_slack_notifications: false,
        closed_trade_retention_ms: None,
    }
}