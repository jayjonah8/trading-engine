#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

use crate::core::{
    config::Config,
    engine::state::EngineState,
    execution::Orders,
    general::General,
    trades::{
        TradeId,
        Trades,
    },
    venue::VenueState,
};

use crate::core::engine::state::LastPrice;


/// ============================================================
/// ENGINE VIEW
/// ============================================================

/// Read-only snapshot published by Engine.
///
/// Strategies and other read-only consumers can clone the watch receiver
/// and inspect the latest Engine state without being able to mutate Engine.
///
/// The snapshot exposes both logical Trade state and canonical Order state.
///
/// Trade state answers questions such as:
///
/// - does this strategy currently own an active Trade?
/// - is the Trade Opening, Open, Closing, or Closed?
/// - what quantity and average entry price does the Trade have?
///
/// Order state answers questions such as:
///
/// - has an entry/exit Order been created?
/// - is the Order PendingSubmit, Working, PendingCancel, etc.?
/// - what canonical OrderId should be used for CancelOrder / ReplaceOrder?
///
/// The Engine remains the only authority allowed to mutate either collection.
#[derive(Debug, Clone)]
pub struct EngineView {
    pub cfg: Config,
    pub general: General,

    /// Canonical Trade state exposed read-only to strategies.
    pub trades: Trades,

    /// Canonical Order state exposed read-only to strategies.
    ///
    /// Strategies may observe execution lifecycle state and obtain canonical
    /// OrderIds for later requests such as CancelOrder or ReplaceOrder.
    ///
    /// This becomes especially important for working limit orders because the
    /// Trade and Order lifecycles are intentionally separate.
    pub orders: Orders,

    /// Latest canonical external venue/account/instrument state.
    ///
    /// The Engine owns and mutates VenueState. Readers only receive cloned
    /// snapshots through EngineView.
    pub venue_state: VenueState,

    /// Convenience field so readers don't have to count every time.
    pub open_trades: usize,

    /// Latest engine-maintained market price for each symbol.
    pub last_px: HashMap<String, LastPrice>,
}


impl EngineView {
    /// Builds a read-only snapshot from Engine configuration,
    /// general state, canonical EngineState, and external VenueState.
    pub fn from_parts(
        cfg: &Config,
        general: &General,
        state: &EngineState,
        venue_state: &VenueState,
        last_px: &HashMap<String, LastPrice>,
    ) -> Arc<Self> {
        Arc::new(
            Self {
                cfg: cfg.clone(),
                general: general.clone(),
                trades: state.trades.clone(),
                orders: state.orders.clone(),
                venue_state: venue_state.clone(),
                last_px: last_px.clone(),
                open_trades: state.trades.count_open(),
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
                general: General::default(),
                trades: Trades::default(),
                orders: Orders::default(),
                venue_state: VenueState::default(),
                last_px: HashMap::new(),
                open_trades: 0,
            }
        )
    }
}