#![allow(dead_code)]

use super::state::MultiSyncWindow;


// ============================================================
// MULTI-SYNC WINDOW TIMER
// ============================================================
//
// PURPOSE:
// Manages time-dependent window behavior using timestamps
// supplied by the strategy.
//
// PLANNED FUNCTIONALITY (PHASE 3):
// - Calculate scheduled window expiration timestamps.
// - Determine whether a regime reversal window has expired.
// - Support finite and indefinite windows.
// - Coordinate scheduled expiration with MultiSync.
//
// IMPORTANT:
// The scheduled expiration and accounting completion
// are separate operations.
//
// SCHEDULED EXPIRATION:
// The strategy returns to its original market regime
// when the configured window duration ends.
//
// ACCOUNTING COMPLETION:
// The first TradeClosed event at or after the scheduled
// expiration advances stages and resets the PnL anchor.
//
// DETERMINISM:
// Uses strategy-supplied timestamps rather than system time.
//
// This allows identical timing logic in backtesting,
// simulation, paper trading, and live trading.
//
// NO INTERNAL TIMER:
// Does not create a Tokio timer or background task.
//
// ============================================================


// ============================================================
// MULTI-SYNC WINDOW TIMER
// ============================================================
//
// PURPOSE:
// Handles scheduled window expiration using timestamps
// supplied by the strategy.
//
// A scheduled expiration ends the regime reversal.
//
// Accounting completion happens separately, on the first
// TradeClosed event at or after the scheduled expiration.
//
// This separation preserves the V2 simulation behavior.
//
// No Tokio timer or background task is required.
//
// ============================================================



// ============================================================
// WINDOW END TIMESTAMP
// ============================================================

/// Calculates the scheduled expiration timestamp.
///
/// None means the window remains active indefinitely.
pub(super) fn calculate_window_end(
    start_ts_ms: i64,
    window_ms: Option<i64>,
) -> Result<Option<i64>, &'static str> {
    match window_ms {
        Some(duration) => {
            if duration <= 0 {
                return Err("Multi-Sync window duration must be positive");
            }

            let end_ts_ms = start_ts_ms
                .checked_add(duration)
                .ok_or("Multi-Sync window timestamp overflow")?;

            Ok(Some(end_ts_ms))
        }

        None => Ok(None),
    }
}


// ============================================================
// REGIME WINDOW STATUS
// ============================================================

/// Returns whether the supplied timestamp falls inside
/// the active regime reversal window.
///
/// The start timestamp is inclusive.
/// The end timestamp is exclusive.
pub(super) fn is_regime_window_active(
    window: &MultiSyncWindow,
    ts_ms: i64,
) -> bool {
    window.includes_entry(ts_ms)
}


// ============================================================
// ACCOUNTING EXPIRATION
// ============================================================

/// Returns whether the window is ready for accounting
/// completion.
///
/// Accounting completion occurs only when a TradeClosed
/// event is processed at or after the scheduled expiration.
pub(super) fn should_complete_accounting(
    window: &MultiSyncWindow,
    exit_ts_ms: i64,
) -> bool {
    window.is_expired(exit_ts_ms)
}