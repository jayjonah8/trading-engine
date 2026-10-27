#![allow(dead_code)]


// ============================================================
// TRADE DURATION WATCHDOG
// ============================================================
//
// Provides strategy-specific protection against trades remaining
// open beyond their configured maximum duration.
//
// The watchdog periodically checks for overdue trades and allows
// the strategy to request closure through the engine.
//
// Features:
// - Configurable percentage buffer beyond trade duration.
// - Configurable interval between watchdog checks.
// - Runtime enable/disable support.
// - Uses canonical event timestamps.
// - Supports live trading and historical backtesting.
// - Ignores trades without configured durations.
// - Ignores trades that are not in TradeState::Open.
// - Does not track individual close requests.
//
// Each strategy owns an independent watchdog instance.
//
// The watchdog does not modify engine state directly.
//
// ============================================================


#[derive(Debug, Clone)]
pub struct TradeDurationWatchdog {
    /// Enables or disables the watchdog.
    pub enabled: bool,

    /// Additional percentage allowed beyond trade duration.
    pub buffer_percent: f64,

    /// Minimum interval between watchdog checks.
    pub check_interval_ms: i64,

    /// Timestamp of the previous watchdog check.
    last_check_ts_ms: Option<i64>,
}


impl TradeDurationWatchdog {

    // ========================================================
    // NEW
    // ========================================================

    pub fn new(
        enabled: bool,
        buffer_percent: f64,
        check_interval_ms: i64,
    ) -> Self {
        Self {
            enabled,
            buffer_percent,
            check_interval_ms,
            last_check_ts_ms: None,
        }
    }


    // ========================================================
    // SHOULD CHECK
    // ========================================================

    /// Returns true when the watchdog should perform another check.
    ///
    /// Uses event timestamps to support deterministic backtesting.
    pub fn should_check(
        &mut self,
        now_ts_ms: i64,
    ) -> bool {

        if !self.enabled {
            return false;
        }

        if let Some(last_ts) = self.last_check_ts_ms {
            if now_ts_ms.saturating_sub(last_ts)
                < self.check_interval_ms
            {
                return false;
            }
        }

        self.last_check_ts_ms = Some(now_ts_ms);

        true
    }


    // ========================================================
    // SHOULD CLOSE
    // ========================================================

    /// Returns true when a trade exceeds its configured duration
    /// plus the additional watchdog buffer.
    pub fn should_close(
        &self,
        opened_ts_ms: Option<i64>,
        duration_secs: Option<u64>,
        now_ts_ms: i64,
    ) -> bool {

        if !self.enabled {
            return false;
        }

        let Some(opened_ts_ms) = opened_ts_ms else {
            return false;
        };

        let Some(duration_secs) = duration_secs else {
            return false;
        };

        if now_ts_ms < opened_ts_ms {
            return false;
        }

        let elapsed_ms =
            now_ts_ms.saturating_sub(opened_ts_ms);

        let duration_ms =
            duration_secs.saturating_mul(1_000);

        let buffer_multiplier =
            1.0 + self.buffer_percent.max(0.0) / 100.0;

        let maximum_duration_ms =
            (duration_ms as f64 * buffer_multiplier) as u64;

        (elapsed_ms as u64) >= maximum_duration_ms
    }


    // ========================================================
    // ENABLE
    // ========================================================

    /// Enables watchdog checks during runtime.
    pub fn enable(&mut self) {
        self.enabled = true;
    }


    // ========================================================
    // DISABLE
    // ========================================================

    /// Disables watchdog checks during runtime.
    pub fn disable(&mut self) {
        self.enabled = false;
    }


    // ========================================================
    // RESET
    // ========================================================

    /// Resets the watchdog check timer.
    pub fn reset(&mut self) {
        self.last_check_ts_ms = None;
    }
}