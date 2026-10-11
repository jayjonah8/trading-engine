#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

use tokio::sync::RwLock;

use crate::core::events::{
    AppEvent,
    AppEventEnvelope,
    Quote,
    TapeTrade,
};


// ============================================================
// SIMULATION MARKET STATE
// ============================================================

/// Shared live market state used by the simulation execution adapter.
///
/// The live market-data pipeline updates this state as new market events
/// arrive. The simulation execution adapter reads the latest market
/// information when simulating order execution.
///
/// This state is separate from authoritative Engine trading state.
/// It contains only the market information required for simulated fills.
///
/// Unlike backtesting, simulation runs continuously using real-time
/// market data without historical synchronization barriers.
#[derive(Debug, Default)]
pub struct SimulationMarketState {
    trades: HashMap<String, TapeTrade>,
    quotes: HashMap<String, Quote>,
}

impl SimulationMarketState {
    /// Creates empty simulation market state.
    pub fn new() -> Self {
        Self {
            trades: HashMap::new(),
            quotes: HashMap::new(),
        }
    }


    // ========================================================
    // UPDATE MARKET EVENT
    // ========================================================

    /// Updates simulation market state from a canonical live event.
    ///
    /// Only trades and quotes are required for market-order simulation.
    /// Completed candles do not directly affect execution pricing.
    pub fn update_event(
        &mut self,
        env: &AppEventEnvelope,
    ) {
        match &env.event {
            AppEvent::TapeTrade(trade) => {
                self.trades.insert(
                    trade.symbol.clone(),
                    trade.clone(),
                );
            }

            AppEvent::Quote(quote) => {
                self.quotes.insert(
                    quote.symbol.clone(),
                    quote.clone(),
                );
            }

            AppEvent::BarClosed(_) => {}
        }
    }


    // ========================================================
    // GET LAST TRADE
    // ========================================================

    /// Returns the latest public tape trade for a symbol.
    pub fn get_trade(
        &self,
        symbol: &str,
    ) -> Option<&TapeTrade> {
        self.trades.get(symbol)
    }


    // ========================================================
    // GET BEST BID / ASK
    // ========================================================

    /// Returns the latest best bid/ask quote for a symbol.
    pub fn get_quote(
        &self,
        symbol: &str,
    ) -> Option<&Quote> {
        self.quotes.get(symbol)
    }
}


// ============================================================
// SHARED SIMULATION MARKET STATE
// ============================================================

/// Thread-safe shared handle used by the live market-data pipeline
/// and the simulation execution adapter.
pub type SharedSimulationMarketState =
    Arc<RwLock<SimulationMarketState>>;