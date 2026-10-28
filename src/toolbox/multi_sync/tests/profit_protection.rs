use crate::toolbox::multi_sync::{
    MultiSyncProfitProtectionState,
    MultiSyncSetting,
};


// ============================================================
// MULTI-SYNC PROFIT PROTECTION TESTS
// ============================================================
//
// PURPOSE:
// Verifies Profit Protection threshold evaluation and
// stage progression behavior.
//
// TESTS:
//
// profit_protection_triggers_at_threshold:
// - Verifies that PnL below the threshold does not trigger.
// - Verifies that PnL equal to the threshold triggers.
// - Verifies that PnL above the threshold triggers.
//
// profit_protection_advances_stages:
// - Verifies advancement between configured stages.
// - Verifies wrapping back to stage zero.
// - Verifies completed cycle counting.
//
// V2 COMPATIBILITY:
// Ensures the profit threshold comparison and stage
// progression behavior match the V2 simulation.
//
// RUN THESE TESTS:
// cargo test toolbox::multi_sync::tests::profit_protection -- --nocapture
//
// ============================================================


#[test]
fn profit_protection_triggers_at_threshold() {
    let state = MultiSyncProfitProtectionState {
        enabled: true,

        settings: vec![
            MultiSyncSetting {
                threshold_pct: 0.98,
                window_ms: Some(5_400_000),
            },
        ],

        current_idx: 0,
        window_triggers: 0,
        completed_cycles: 0,
    };

    assert!(!state.should_trigger(0.97));
    assert!(state.should_trigger(0.98));
    assert!(state.should_trigger(1.20));
}


#[test]
fn profit_protection_advances_stages() {
    let mut state = MultiSyncProfitProtectionState {
        enabled: true,

        settings: vec![
            MultiSyncSetting {
                threshold_pct: 0.98,
                window_ms: Some(5_400_000),
            },
            MultiSyncSetting {
                threshold_pct: 0.50,
                window_ms: Some(3_600_000),
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