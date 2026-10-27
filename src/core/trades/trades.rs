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


    /// Returns true if the given strategy already has an active logical trade
    /// for the specified symbol.
    ///
    /// `Opening`, `Open`, and `Closing` trades are all considered active.
    ///
    /// This prevents the same strategy from accidentally creating multiple
    /// independent `Trade` lifecycles for the same symbol. It does NOT restrict
    /// the number of market, limit, stop, or exit `Order`s associated with that
    /// trade. A single active `Trade` may own many simultaneous orders.
    pub fn has_active_trade_for_strategy_symbol(
        &self,
        strategy_id: &str,
        symbol: &str,
    ) -> bool {
        self.trades.values().any(|trade| {
            trade.strategy_id == strategy_id
                && trade.symbol == symbol
                && trade.state.is_active()
        })
    }


    /// Returns the active TradeId owned by a specific strategy for a symbol.
    ///
    /// Strategies often make decisions using symbols, while engine commands use
    /// canonical `TradeId`s so multiple strategies or trades on the same symbol
    /// cannot be confused.
    ///
    /// Returns `None` when no active matching trade exists.
    pub fn active_trade_id_for_strategy_symbol(
        &self,
        strategy_id: &str,
        symbol: &str,
    ) -> Option<TradeId> {
        self.trades
            .values()
            .find(|trade| {
                trade.strategy_id == strategy_id
                    && trade.symbol == symbol
                    && trade.state.is_active()
            })
            .map(|trade| trade.id)
    }

    
    // ============================================================
    // OPEN TRADES FOR STRATEGY
    // ============================================================

    /// Iterates over fully open trades belonging to a specific strategy.
    ///
    /// Only trades in TradeState::Open are returned.
    ///
    /// Trades belonging to other strategies are excluded.
    ///
    /// Useful for strategy-specific trade management, including
    /// duration watchdogs and custom exit conditions.
    pub fn open_trades_for_strategy<'a>(
        &'a self,
        strategy_id: &'a str,
    ) -> impl Iterator<Item = &'a Trade> + 'a {
        self.trades
            .values()
            .filter(move |trade| {
                trade.strategy_id == strategy_id
                    && trade.state == TradeState::Open
            })
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


    /// Returns the number of logical trades with remaining
    /// executed quantity.
    ///
    /// Includes partially filled Opening trades and partially
    /// exited Closing trades.
    ///
    /// Unlike count_open(), this measures remaining trade
    /// exposure rather than lifecycle state.
    pub fn count_with_open_quantity(&self) -> usize {
        self.iter_with_open_quantity().count()
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

    /// Iterates over all trades with mutable access.
    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&TradeId, &mut Trade)> {
        self.trades.iter_mut()
    }


    /// Iterates over all active trades.
    ///
    /// Active trades are:
    ///
    /// - Opening
    /// - Open
    /// - Closing
    ///
    /// Closed trades are excluded.
    ///
    /// This helper keeps active-trade filtering centralized so risk checks do not
    /// duplicate lifecycle-state logic.
    pub fn iter_active(
        &self,
    ) -> impl Iterator<Item = &Trade> {
        self.trades
            .values()
            .filter(
                |trade| trade.state.is_active()
            )
    }

  
    /// Iterates over trades that still have remaining
    /// executed quantity.
    ///
    /// Includes Opening, Open, and Closing trades when
    /// their remaining quantity is positive.
    pub fn iter_with_open_quantity(
        &self,
    ) -> impl Iterator<Item = &Trade> {
        self.trades
            .values()
            .filter(|trade| {
                trade.quantity > f64::EPSILON
            })
    }
}