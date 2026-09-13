use std::sync::Arc;

use crate::core::{
    config::Config,
    engine::state::EngineState,
    general::General,
    trades::{
        TradeId,
        Trades,
    },
};

/// Read-only snapshot published by Engine.
///
/// Strategies and pollers can clone the watch receiver and inspect
/// the latest Engine state without being able to mutate the Engine.
#[derive(Debug, Clone)]
pub struct EngineView {
    pub cfg: Config,
    pub general: General,
    pub trades: Trades,

    /// Convenience field so readers don't have to count every time.
    pub open_trades: usize,
}

impl EngineView {
    /// Builds a read-only snapshot from Engine configuration,
    /// general account state, and canonical EngineState.
    pub fn from_parts(
        cfg: &Config,
        general: &General,
        state: &EngineState,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg: cfg.clone(),
            general: general.clone(),
            trades: state.trades.clone(),
            open_trades: state.trades.count_open(),
        })
    }

    /// Returns the active TradeId owned by a specific strategy for a symbol.
    ///
    /// Strategies often make decisions using symbols, while engine commands use
    /// canonical TradeIds so multiple strategies or trades on the same symbol
    /// cannot be confused.
    ///
    /// This is a read-only lookup against the published EngineView snapshot.
    ///
    /// Returns `None` when no active matching trade exists.
    pub fn active_trade_id_for_strategy_symbol(
        &self,
        strategy_id: &str,
        symbol: &str,
    ) -> Option<TradeId> {
        self.trades
            .active_trade_id_for_strategy_symbol(
                strategy_id,
                symbol,
            )
    }

    /// Placeholder snapshot used only when creating the watch channel.
    ///
    /// The caller must provide an explicit Config because execution venue
    /// selection intentionally has no implicit default.
    pub fn empty(
        cfg: Config,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            general: General::default(),
            trades: Trades::default(),
            open_trades: 0,
        })
    }
}