#![allow(dead_code)]

use crate::core::{
    execution::{
        order::{
            id::OrderId,
            order_type::OrderType,
            pending_replace::PendingOrderReplace,
            purpose::OrderPurpose,
            state::OrderState,
            time_in_force::TimeInForce,
        },
        side::Side,
        Venue,
    },
    trades::TradeId,
};

/// Canonical engine representation of an execution order.
///
/// `Order` is venue-independent and represents the complete lifecycle of an
/// order regardless of whether it is executed through Alpaca, IBKR,
/// Hyperliquid, a simulated venue, or another future adapter.
///
/// The engine owns this state. Exchange adapters report execution events but
/// should not directly mutate an `Order`.
///
/// Multiple orders may exist simultaneously for the same symbol, strategy,
/// or trade.
///
/// 
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `String` venue -> `Venue`
/// - `String` strategy ID -> `StrategyId`
/// - `f64` quantities -> `Quantity`
/// - `f64` prices -> `Price`
///
/// Primitive types are intentionally being used during the initial execution
/// engine refactor to avoid introducing too many abstractions at once.
///
/// Strong types will later improve type safety and allow `Price` and `Quantity`
/// to enforce venue/instrument requirements such as tick size, quantity step,
/// precision, and exact execution accounting.
#[derive(Debug, Clone)]
pub struct Order {
    /// Internal engine-owned order identifier.
    pub id: OrderId,

    /// Venue responsible for executing this order.
    pub venue: Venue,

    /// Order identifier assigned by the execution venue.
    ///
    /// This may not exist until the venue acknowledges the order.
    pub venue_order_id: Option<String>,

    /// Client order identifier sent to venues that support one.
    ///
    /// This is separate from the internal `OrderId`.
    pub client_order_id: String,

    /// Strategy that created the order.
    ///
    /// TODO: Replace with `StrategyId`.
    pub strategy_id: String,

    /// Logical trade associated with this order.
    ///
    /// `None` is valid for orders that do not belong to a directional trade,
    /// such as market-making quotes.
    pub trade_id: Option<TradeId>,

    /// Instrument symbol being traded.
    pub symbol: String,

    /// Buy or sell execution side.
    pub side: Side,

    /// Engine-level reason this order exists.
    pub purpose: OrderPurpose,

    /// Execution type of the order.
    pub order_type: OrderType,

    /// Time-in-force policy.
    pub time_in_force: TimeInForce,

    /// Total requested quantity.
    ///
    /// TODO: Replace with `Quantity`.
    pub quantity: f64,

    /// Limit price when required by the order type.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub limit_px: Option<f64>,

    /// Stop trigger price when required by the order type.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub stop_px: Option<f64>,

    /// Prevents the order from increasing exposure when supported by the venue.
    pub reduce_only: bool,

    /// Prevents the order from immediately taking liquidity when supported
    /// by the venue.
    pub post_only: bool,

    /// Total quantity filled across all executions for this order.
    ///
    /// A value between zero and `quantity` means the order has been
    /// partially filled.
    ///
    /// TODO: Replace with `Quantity`.
    pub filled_quantity: f64,

    /// Volume-weighted average execution price across all fills.
    ///
    /// `None` means the order has not received a fill yet.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub avg_fill_px: Option<f64>,

    /// Replacement request currently awaiting venue confirmation.
    ///
    /// `Some(...)` means the engine has requested an asynchronous modification
    /// but has not yet received confirmation that the venue applied it.
    ///
    /// The canonical order fields are not changed until `OrderReplaced` confirms
    /// the replacement.
    pub pending_replace: Option<PendingOrderReplace>,

    /// Current engine lifecycle state.
    pub state: OrderState,

    /// Engine timestamp when the order was created.
    pub created_ts_ms: i64,

    /// Timestamp of the most recent lifecycle update.
    pub updated_ts_ms: i64,
}

impl Order {
    /// Returns the quantity that has not yet been filled.
    pub fn remaining_quantity(&self) -> f64 {
        (self.quantity - self.filled_quantity).max(0.0)
    }

    /// Returns true when the order has received at least one fill but has not
    /// yet filled its entire requested quantity.
    pub fn is_partially_filled(&self) -> bool {
        self.filled_quantity > 0.0 && self.filled_quantity < self.quantity
    }

    /// Returns true when the order has reached a terminal lifecycle state.
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}