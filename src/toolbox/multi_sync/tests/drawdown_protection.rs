use crate::toolbox::multi_sync::{
    MultiSyncDrawdownProtectionState,
    MultiSyncSetting,
};


// ============================================================
// MULTI-SYNC DRAWDOWN PROTECTION TESTS
// ============================================================
//
// PURPOSE:
// Verifies Drawdown Protection threshold evaluation and
// stage progression behavior.
//
// TESTS:
//
// drawdown_protection_triggers_at_threshold:
// - Verifies that PnL above the negative threshold
//   does not trigger.
// - Verifies that PnL equal to the negative threshold triggers.
// - Verifies that PnL below the negative threshold triggers.
//
// drawdown_protection_advances_stages:
// - Verifies advancement between configured stages.
// - Verifies wrapping back to stage zero.
// - Verifies completed cycle counting.
//
// V2 COMPATIBILITY:
// Ensures the drawdown threshold comparison and stage
// progression behavior match the V2 simulation.
//
// RUN THESE TESTS:
// cargo test toolbox::multi_sync::tests::drawdown_protection -- --nocapture
//
// ============================================================


#[test]
fn drawdown_protection_triggers_at_threshold() {
    let state = MultiSyncDrawdownProtectionState {
        enabled: true,

        settings: vec![
            MultiSyncSetting {
                threshold_pct: 0.24,
                window_ms: Some(14_400_000),
            },
        ],

        current_idx: 0,
        window_triggers: 0,
        completed_cycles: 0,
    };

    assert!(!state.should_trigger(-0.23));
    assert!(state.should_trigger(-0.24));
    assert!(state.should_trigger(-0.50));
}


#[test]
fn drawdown_protection_advances_stages() {
    let mut state = MultiSyncDrawdownProtectionState {
        enabled: true,

        settings: vec![
            MultiSyncSetting {
                threshold_pct: 0.24,
                window_ms: Some(14_400_000),
            },
            MultiSyncSetting {
                threshold_pct: 0.50,
                window_ms: Some(7_200_000),
            },
        ],

        current_idx: 0,
        window_triggers: 0,
        completed_cycles: 0,
    };

    state.advance_stage();

    assert_eq!(state.current_idx, 1);
    assert_eq!(state.completed_cycles, 0);

    state.advance_stage();

    assert_eq!(state.current_idx, 0);
    assert_eq!(state.completed_cycles, 1);
}