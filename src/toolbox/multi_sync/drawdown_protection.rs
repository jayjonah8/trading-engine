#![allow(dead_code)]

use super::state::MultiSyncDrawdownProtectionState;


// ============================================================
// MULTI-SYNC DRAWDOWN PROTECTION
// ============================================================
//
// PURPOSE:
// Evaluates negative realized PnL pressure against the
// currently configured Drawdown Protection threshold.
//
// TRIGGER CONDITION:
// pnl_pct <= -current_setting.threshold_pct
//
// The threshold is evaluated using realized PnL accumulated
// since the shared Multi-Sync anchor.
//
// CURRENT FUNCTIONALITY:
// - should_trigger():
//     Checks whether the current drawdown threshold is reached.
//
// - current_setting():
//     Returns the current stage configuration.
//
// - advance_stage():
//     Advances to the next configured stage and wraps
//     to stage zero after the final stage.
//
// - reset_stage():
//     Resets the current stage index to zero.
//
// STAGE PROGRESSION:
// Stages advance when a Drawdown Protection window completes
// its accounting lifecycle, not when initially triggered.
//
// CROSS-PROTECTION BEHAVIOR:
// When Profit Protection completes a window, Drawdown
// Protection resets to its first stage.
//
// CYCLE TRACKING:
// A completed cycle is recorded whenever stage progression
// wraps back to stage zero.
//
// V2 COMPATIBILITY:
// Preserves the negative threshold evaluation and
// stage progression behavior of the V2 simulation.
//
// ============================================================


impl MultiSyncDrawdownProtectionState {
    // ========================================================
    // THRESHOLD EVALUATION
    // ========================================================

    /// Returns whether cumulative realized PnL pressure
    /// has reached the current Drawdown Protection threshold.
    pub fn should_trigger(&self, pnl_pct: f64) -> bool {
        if !self.enabled {
            return false;
        }

        let Some(setting) = self.settings.get(self.current_idx) else {
            return false;
        };

        pnl_pct <= -setting.threshold_pct
    }


    // ========================================================
    // CURRENT SETTING
    // ========================================================

    /// Returns the configuration for the current stage.
    pub fn current_setting(&self) -> Option<&super::config::MultiSyncSetting> {
        self.settings.get(self.current_idx)
    }


    // ========================================================
    // STAGE ADVANCEMENT
    // ========================================================

    /// Advances to the next stage after a Drawdown Protection
    /// window completes its accounting lifecycle.
    ///
    /// Wraps to stage zero after the final stage.
    pub fn advance_stage(&mut self) {
        if self.settings.is_empty() {
            return;
        }

        self.current_idx = (self.current_idx + 1) % self.settings.len();

        if self.current_idx == 0 {
            self.completed_cycles += 1;
        }
    }


    // ========================================================
    // STAGE RESET
    // ========================================================

    /// Resets Drawdown Protection to its first stage.
    ///
    /// Used when a Profit Protection window completes.
    pub fn reset_stage(&mut self) {
        self.current_idx = 0;
    }
}