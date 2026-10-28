

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