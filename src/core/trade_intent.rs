#![allow(dead_code)]

use crate::core::{
    execution::order::{
        OrderType,
        TimeInForce,
    },
    trades::Direction,
};

/// Strategy request to create a new directional trade.
///
/// `TradeIntent` describes:
///
/// - what directional exposure the strategy wants
/// - how the initial entry should be executed
/// - optional trade-level risk rules
///
/// It does not represent an exchange order itself.
///
/// The engine converts this intent into:
///
/// TradeIntent
///     -> Trade
///     -> RiskReservation
///     -> Order
///     -> ExecCommand
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `String` strategy ID -> `StrategyId`
/// - `String` symbol -> `Symbol`
/// - `f64` prices -> `Price`
/// - `f64` monetary values -> `Money`
#[derive(Debug, Clone)]
pub struct TradeIntent {
    /// Strategy generating this trade request.
    ///
    /// TODO: Replace with `StrategyId`.
    pub strategy_id: String,

    /// Instrument to trade.
    ///
    /// TODO: Replace with canonical `Symbol`.
    pub symbol: String,

    /// Directional exposure requested by the strategy.
    pub direction: Direction,

    /// Market/reference price observed when the strategy generated the intent.
    ///
    /// This is useful for recording strategy signal price and diagnostics.
    ///
    /// TODO: Replace with `Price`.
    pub signal_px: f64,

    /// Requested notional size of the trade.
    ///
    /// TODO: Replace with `Money`.
    pub target_notional: f64,

    // ============================================================
    // ENTRY EXECUTION
    // ============================================================

    /// Type of order to use for the initial entry.
    pub order_type: OrderType,

    /// Time-in-force applied to the entry order.
    pub time_in_force: TimeInForce,

    /// Limit price when required by the selected `OrderType`.
    ///
    /// Expected for:
    ///
    /// - `OrderType::Limit`
    /// - `OrderType::StopLimit`
    ///
    /// TODO: Replace with `Option<Price>`.
    pub limit_px: Option<f64>,

    /// Stop trigger price when required by the selected `OrderType`.
    ///
    /// Expected for:
    ///
    /// - `OrderType::StopMarket`
    /// - `OrderType::StopLimit`
    ///
    /// TODO: Replace with `Option<Price>`.
    pub stop_px: Option<f64>,

    /// Prevents the entry order from immediately taking liquidity when
    /// supported by the execution venue.
    pub post_only: bool,

    /// Normally false for entry orders.
    ///
    /// Included so canonical order construction can remain universal.
    pub reduce_only: bool,

    // ============================================================
    // TRADE RISK PLAN
    // ============================================================

    /// Optional take-profit percentage managed by the engine.
    ///
    /// `None` means the strategy does not use the engine's percentage-based
    /// take-profit logic.
    pub take_profit_percent: Option<f64>,

    /// Optional normal stop-loss percentage managed by the engine.
    ///
    /// `None` means no engine-managed percentage stop loss.
    pub stop_loss_percent: Option<f64>,

    /// Optional emergency stop-loss percentage.
    ///
    /// `None` disables the engine-managed emergency stop.
    pub emergency_stop_percent: Option<f64>,

    /// Optional delay before the normal stop-loss becomes active.
    ///
    /// For example, `Some(30)` means the normal stop-loss cannot trigger
    /// until 30 seconds after the trade becomes open.
    pub stop_loss_delay_secs: Option<u64>,

    /// Optional maximum lifetime of the trade.
    ///
    /// `None` means there is no engine-managed duration exit.
    pub max_duration_secs: Option<u64>,

    /// Event timestamp when the strategy generated the intent.
    ///
    /// Live trading uses live event time while historical replay uses the
    /// original historical timestamp.
    pub ts_ms: i64,
}