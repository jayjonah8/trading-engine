use std::sync::Arc;

use crate::core::{
    config::Config,
    engine::state::EngineState,
    general::General,
    trades::{
        TradeId,
        Trades,
    },
    venue_state::VenueState,
};


/// ============================================================
/// ENGINE VIEW
/// ============================================================

/// Read-only snapshot published by Engine.
///
/// Strategies and other read-only consumers can clone the watch receiver
/// and inspect the latest Engine state without being able to mutate Engine.
#[derive(Debug, Clone)]
pub struct EngineView {
    pub cfg: Config,

    pub general: General,

    /// Canonical active Trade state exposed to strategies.
    pub trades: Trades,

    /// Latest canonical external venue/account/instrument state.
    ///
    /// The Engine owns and mutates VenueState. Readers only receive cloned
    /// snapshots through EngineView.
    pub venue_state: VenueState,

    /// Convenience field so readers don't have to count every time.
    pub open_trades: usize,
}


impl EngineView {
    /// Builds a read-only snapshot from Engine configuration,
    /// general state, canonical EngineState, and external VenueState.
    pub fn from_parts(
        cfg: &Config,
        general: &General,
        state: &EngineState,
        venue_state: &VenueState,
    ) -> Arc<Self> {
        Arc::new(
            Self {
                cfg: cfg.clone(),

                general: general.clone(),

                trades:
                    state.trades.clone(),

                venue_state:
                    venue_state.clone(),

                open_trades:
                    state.trades.count_open(),
            }
        )
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
        Arc::new(
            Self {
                cfg,

                general:
                    General::default(),

                trades:
                    Trades::default(),

                venue_state:
                    VenueState::default(),

                open_trades: 0,
            }
        )
    }
}