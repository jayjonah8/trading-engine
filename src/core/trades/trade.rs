#![allow(dead_code)]

use std::time::{Instant, SystemTime};

use crate::core::trades::{
    direction::Direction,
    id::TradeId,
    state::TradeState,
};

// Trade should represent the strategy lifecycle, 
// while orders/fills/positions handle execution state.

/// Logical strategy-level representation of a directional trade.
///
/// A `Trade` represents the lifecycle and risk plan of a strategy trade.
/// It does NOT represent an exchange order or the engine's authoritative
/// venue position.
///
/// Execution details belong to:
///
/// - `Order`    -> what the engine asked a venue to execute
/// - `Fill`     -> what actually executed
/// - `Position` -> actual venue/account exposure
/// - `Trade`    -> why the strategy holds that exposure and how it manages it
///
/// A single trade may create multiple orders throughout its lifetime.
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `String` strategy ID -> `StrategyId`
/// - `String` symbol -> `Symbol`
/// - `f64` quantity -> `Quantity`
/// - `f64` prices -> `Price`
/// - `f64` monetary values -> `Money`
#[derive(Debug, Clone)]
pub struct Trade {
    /// Internal engine-owned trade identifier.
    pub id: TradeId,

    /// Strategy that owns this trade.
    ///
    /// TODO: Replace with `StrategyId`.
    pub strategy_id: String,

    /// Instrument being traded.
    ///
    /// TODO: Replace with canonical `Symbol`.
    pub symbol: String,

    /// Directional exposure intended by the strategy.
    pub direction: Direction,

    /// Current logical trade lifecycle state.
    pub state: TradeState,

    /// Current quantity established for this trade.
    ///
    /// Unlike `Position`, this quantity is trade-specific and is always
    /// represented as a positive magnitude. Direction is stored separately.
    ///
    /// TODO: Replace with `Quantity`.
    pub quantity: f64,

    /// Average entry execution price accumulated from confirmed entry fills.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub avg_entry_px: Option<f64>,

    /// Average exit execution price accumulated from confirmed exit fills.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub avg_exit_px: Option<f64>,

    /// Strategy market price when the entry intent was created.
    ///
    /// TODO: Replace with `Price`.
    pub entry_signal_px: f64,

    /// Market price that triggered the strategy's exit decision.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub exit_signal_px: Option<f64>,

    /// Requested notional size of the trade.
    ///
    /// TODO: Replace with `Money`.
    pub target_notional: f64,

    /// Optional take-profit percentage.
    pub take_profit_percent: Option<f64>,

    /// Optional stop-loss percentage.
    pub stop_loss_percent: Option<f64>,

    /// Optional emergency stop percentage.
    pub emergency_stop_percent: Option<f64>,

    /// Optional delay before the normal stop-loss becomes active.
    pub stop_loss_delay_secs: Option<u64>,

    /// Optional maximum lifetime of the trade.
    ///
    /// `None` means no duration-based exit is configured.
    pub max_duration_secs: Option<u64>,

    /// Serializable timestamp when the trade was created.
    pub created_ts_ms: i64,

    /// Timestamp of the first confirmed entry fill.
    pub opened_ts_ms: Option<i64>,

    /// Timestamp when the final remaining exposure was closed.
    pub closed_ts_ms: Option<i64>,

    /// Monotonic timer captured when the trade becomes open.
    ///
    /// Used for reliable elapsed-duration checks during the current process.
    pub entry_time: Option<Instant>,

    /// Real-world clock time captured when the trade becomes open.
    pub entry_wall_time: Option<SystemTime>,
}

