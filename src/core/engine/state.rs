use crate::core::{
    execution::{
        FillLedger,
        OrderId,
        Orders,
    },
    positions::Positions,
    risk::{
        RiskLimits,
        RiskState,
    },
    trades::{
        TradeId,
        Trades,
    },
};

/// Authoritative mutable state owned by the trading engine.
///
/// `EngineState` is the single source of truth for core trading state.
///
/// Exchange adapters, strategies, market-data providers, and notification
/// systems must not directly mutate this state. They communicate with the
/// engine through events and commands, and the engine's handlers perform the
/// actual state transitions.
///
/// Keeping authoritative state in one place avoids scattered synchronized
/// maps and makes execution behavior deterministic and easier to reason about.
#[derive(Debug)]
pub struct EngineState {
    /// All execution orders known by the engine.
    ///
    /// Includes active and terminal orders until they are persisted or
    /// archived.
    pub orders: Orders,

    /// Immutable executions already accepted by the engine.
    ///
    /// Used for execution accounting and duplicate-fill protection.
    pub fills: FillLedger,

    /// Actual venue/account exposure produced by confirmed fills or
    /// reconciliation.
    pub positions: Positions,

    /// Logical strategy-level directional trade lifecycles.
    pub trades: Trades,

    /// Risk limits and active execution reservations.
    pub risk: RiskState,

    /// Next internal engine order ID.
    ///
    /// ID allocation belongs to the engine rather than exchange adapters.
    next_order_id: u64,

    /// Next internal engine trade ID.
    next_trade_id: u64,
}

impl EngineState {
    /// Creates empty engine state using the supplied risk limits.
    pub fn new(risk_limits: RiskLimits) -> Self {
        Self {
            orders: Orders::new(),
            fills: FillLedger::new(),
            positions: Positions::new(),
            trades: Trades::new(),
            risk: RiskState::new(risk_limits),

            // Start at 1 so zero can remain unused/reserved if desired.
            next_order_id: 1,
            next_trade_id: 1,
        }
    }

    /// Allocates a new engine-owned order ID.
    ///
    /// Venue-specific order IDs remain completely separate and are stored
    /// on the associated `Order`.
    pub fn allocate_order_id(&mut self) -> OrderId {
        let id = OrderId::new(self.next_order_id);

        self.next_order_id = self
            .next_order_id
            .checked_add(1)
            .expect("OrderId allocator overflow");

        id
    }

    /// Allocates a new engine-owned logical trade ID.
    ///
    /// One `TradeId` may be associated with multiple `OrderId`s throughout
    /// the lifecycle of a directional trade.
    pub fn allocate_trade_id(&mut self) -> TradeId {
        let id = TradeId::new(self.next_trade_id);

        self.next_trade_id = self
            .next_trade_id
            .checked_add(1)
            .expect("TradeId allocator overflow");

        id
    }
}

impl Default for EngineState {
    fn default() -> Self {
        Self::new(RiskLimits::default())
    }
}