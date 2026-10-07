#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

use tokio::sync::RwLock;

use crate::core::engine::state::LastPrice;


// ============================================================
// BACKTEST MARKET STATE
// ============================================================

/// Shared historical market state used by the backtest execution adapter.
///
/// The historical replay updates this state before strategies process each
/// market event. The backtest execution adapter reads the latest price and
/// historical timestamp when simulating execution.
///
/// This state is separate from authoritative Engine trading state. It contains
/// only the market information required to simulate execution.
#[derive(Debug, Default)]
pub struct BacktestMarketState {
    prices: HashMap<String, LastPrice>,
}

impl BacktestMarketState {
    /// Creates empty backtest market state.
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
        }
    }

    /// Updates the latest historical market price for a symbol.
    pub fn update_price(
        &mut self,
        symbol: impl Into<String>,
        px: f64,
        ts_ms: i64,
    ) {
        self.prices.insert(
            symbol.into(),
            LastPrice {
                px,
                ts_ms,
            },
        );
    }

    /// Returns the latest historical market price for a symbol.
    pub fn get_price(
        &self,
        symbol: &str,
    ) -> Option<LastPrice> {
        self.prices
            .get(symbol)
            .copied()
    }
}


// ============================================================
// SHARED BACKTEST MARKET STATE
// ============================================================

/// Thread-safe shared handle used by historical replay and the backtest
/// execution adapter.
pub type SharedBacktestMarketState =
    Arc<RwLock<BacktestMarketState>>;