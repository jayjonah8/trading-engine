#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::trades::{
    id::TradeId,
    state::TradeState,
    trade::Trade,
};

/// Authoritative collection of strategy trades known by the engine.
///
/// `Trades` represents logical strategy-level trade lifecycles. It does not
/// represent venue orders or actual account exposure.
///
/// Those responsibilities belong to:
///
/// - `Orders`    -> execution requests and order lifecycle
/// - `FillLedger` -> confirmed executions
/// - `Positions` -> actual venue/account exposure
/// - `Trades`    -> strategy-level trade lifecycle
///
/// Unlike the old architecture, pending opens and pending closes are not
/// stored here. Those states are represented by `Order` and `OrderState`.
#[derive(Debug, Default, Clone)]
pub struct Trades {
    trades: HashMap<TradeId, Trade>,
}

impl Trades {
    /// Creates an empty trade collection.
    pub fn new() -> Self {
        Self {
            trades: HashMap::new(),
        }
    }

    /// Inserts a new trade or replaces an existing trade with the same ID.
    pub fn insert(&mut self, trade: Trade) -> Option<Trade> {
        self.trades.insert(trade.id, trade)
    }

    /// Returns a trade by its internal engine trade ID.
    pub fn get(&self, trade_id: TradeId) -> Option<&Trade> {
        self.trades.get(&trade_id)
    }

    /// Returns mutable access to a trade.
    ///
    /// Engine state handlers should be the primary users of mutable access.
    pub fn get_mut(&mut self, trade_id: TradeId) -> Option<&mut Trade> {
        self.trades.get_mut(&trade_id)
    }

    /// Returns true if an active trade exists for the given symbol.
    ///
    /// `Opening`, `Open`, and `Closing` trades are all considered active.
    pub fn has_active_trade_for_symbol(&self, symbol: &str) -> bool {
        self.trades
            .values()
            .any(|trade| trade.symbol == symbol && trade.state.is_active())
    }

    /// Returns the number of active strategy trades.
    ///
    /// Active trades include trades that are `Opening`, `Open`, or `Closing`.
    ///
    /// This should NOT be used by itself for production risk gating because
    /// `RiskState` also accounts for reserved exposure from working orders.
    pub fn count_active(&self) -> usize {
        self.trades.values().filter(|trade| trade.state.is_active()).count()
    }

    /// Returns the number of fully open strategy trades.
    ///
    /// Unlike `count_active()`, this only counts trades in the `Open` state.
    /// Trades that are still `Opening`, already `Closing`, or `Closed` are excluded.
    pub fn count_open(&self) -> usize {
        self.trades.values().filter(|trade| trade.state == TradeState::Open).count()
    }

    /// Marks a trade as closed.
    ///
    /// This should only occur after confirmed fills have reduced the trade's
    /// remaining exposure to zero.
    pub fn mark_closed(&mut self, trade_id: TradeId, ts_ms: i64) -> bool {
        let Some(trade) = self.trades.get_mut(&trade_id) else {
            return false;
        };

        trade.state = TradeState::Closed;
        trade.closed_ts_ms = Some(ts_ms);

        true
    }

    /// Removes a trade from the collection.
    ///
    /// Normally closed trades should remain available until they have been
    /// persisted or archived rather than being removed immediately.
    pub fn remove(&mut self, trade_id: TradeId) -> Option<Trade> {
        self.trades.remove(&trade_id)
    }

    /// Returns the total number of trade records, including closed trades.
    pub fn len(&self) -> usize {
        self.trades.len()
    }

    /// Returns true when no trade records exist.
    pub fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    /// Iterates over all trades.
    pub fn iter(&self) -> impl Iterator<Item = (&TradeId, &Trade)> {
        self.trades.iter()
    }
}