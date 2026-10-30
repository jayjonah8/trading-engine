#![allow(dead_code)]

use std::collections::HashSet;
use crate::core::trades::TradeId;

use super::config::MultiSyncSetting;


// ============================================================
// MULTI-SYNC STATE
// ============================================================
//
// PURPOSE:
// Stores the runtime state required by Multi-Sync.
//
// SHARED PNL:
// - running_realized_pnl:
//     Cumulative realized PnL belonging to the strategy.
//
// - anchor_realized_pnl:
//     Reference PnL used to calculate trigger pressure.
//
// Both protection systems use the same PnL and anchor.
//
// WINDOW STATE:
// - active_window:
//     Current window, including windows that have passed
//     their scheduled expiration but are still awaiting
//     accounting completion.
//
// - historical_windows:
//     Previously triggered windows used to determine whether
//     completed trades originally entered during a window.
//
// PROTECTION STATE:
// Profit Protection and Drawdown Protection independently
// track their settings, current stage, trigger count,
// and completed cycle count.
//
// WINDOW LIFECYCLE:
// A scheduled expiration ends the regime reversal.
//
// Accounting completion occurs on the first TradeClosed
// event at or after the scheduled expiration.
//
// Stage progression and anchor resets occur during
// accounting completion, matching the V2 simulation.
//
// MEMORY MANAGEMENT:
// TODO: Optimize historical window retention by pruning
// windows no longer referenced by open trades, or by
// storing window classification directly on each trade.
//
// ============================================================


// ============================================================
// MULTI-SYNC WINDOW KIND
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiSyncWindowKind {
    ProfitProtection,
    DrawdownProtection,
}


// ============================================================
// MULTI-SYNC WINDOW
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncWindow {
    /// Protection system that triggered the window.
    pub kind: MultiSyncWindowKind,

    /// Timestamp when the window was triggered.
    pub start_ts_ms: i64,

    /// Scheduled expiration timestamp.
    /// None means the window is indefinite.
    pub end_ts_ms: Option<i64>,
}


// ============================================================
// MULTI-SYNC WINDOW HELPERS
// ============================================================

impl MultiSyncWindow {
    /// Returns whether a trade entered during this window.
    ///
    /// Matches V2's inclusive start and exclusive end.
    pub fn includes_entry(&self, entry_ts_ms: i64) -> bool {
        entry_ts_ms >= self.start_ts_ms
            && self
                .end_ts_ms
                .map_or(true, |end| entry_ts_ms < end)
    }

    /// Returns whether the scheduled window has expired.
    pub fn is_expired(&self, ts_ms: i64) -> bool {
        self.end_ts_ms
            .map_or(false, |end| ts_ms >= end)
    }
}


// ============================================================
// MULTI-SYNC PROFIT PROTECTION STATE
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncProfitProtectionState {
    pub enabled: bool,

    pub settings: Vec<MultiSyncSetting>,

    pub current_idx: usize,

    pub window_triggers: u64,

    pub completed_cycles: u64,
}


// ============================================================
// MULTI-SYNC DRAWDOWN PROTECTION STATE
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncDrawdownProtectionState {
    pub enabled: bool,

    pub settings: Vec<MultiSyncSetting>,

    pub current_idx: usize,

    pub window_triggers: u64,

    pub completed_cycles: u64,
}


// ============================================================
// MULTI-SYNC STATE
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncState {
    /// Cumulative realized PnL belonging to this strategy.
    pub running_realized_pnl: f64,

    /// Reference PnL for calculating trigger pressure.
    pub anchor_realized_pnl: f64,

    /// Active or pending-accounting window.
    ///
    /// A window may remain here after its scheduled expiration
    /// until the first qualifying TradeClosed event.
    pub active_window: Option<MultiSyncWindow>,

    /// Historical windows used to classify completed trades
    /// by their original entry timestamps.
    ///
    /// TODO: Optimize memory management by pruning historical
    /// windows once no remaining open trade can reference them,
    /// or by recording window classification at trade entry.
    pub historical_windows: Vec<MultiSyncWindow>,

    pub processed_trade_ids: HashSet<TradeId>,

    pub profit_protection: MultiSyncProfitProtectionState,

    pub drawdown_protection: MultiSyncDrawdownProtectionState,
}