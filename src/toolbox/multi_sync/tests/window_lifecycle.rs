use crate::toolbox::{
    MarketRegime,
    MultiSync,
};

use crate::toolbox::multi_sync::{
    MultiSyncConfig,
    MultiSyncSetting,
    MultiSyncProfitProtectionConfig,
    MultiSyncDrawdownProtectionConfig,
};


// ============================================================
// MULTI-SYNC WINDOW LIFECYCLE TESTS
// ============================================================
//
// PURPOSE:
// Verifies V2 window activation, regime switching,
// scheduled expiration, accounting completion,
// anchor resets, and retrigger prevention.
//
// RUN:
// cargo test toolbox::multi_sync::tests::window_lifecycle -- --nocapture
//
// ============================================================


// ============================================================
// TEST CONFIGURATION
// ============================================================

fn create_multi_sync() -> MultiSync {
    MultiSync::new(
        MultiSyncConfig {
            enabled: true,

            profit_protection: MultiSyncProfitProtectionConfig {
                enabled: true,

                default_settings: vec![
                    MultiSyncSetting {
                        threshold_pct: 1.0,
                        window_ms: Some(90_000),
                    },
                ],
            },

            drawdown_protection: MultiSyncDrawdownProtectionConfig {
                enabled: true,

                default_settings: vec![
                    MultiSyncSetting {
                        threshold_pct: 1.0,
                        window_ms: Some(120_000),
                    },
                ],
            },

            debug_mode: false,
        },

        MarketRegime::MeanReversion,
    )
    .unwrap()
}


// ============================================================
// TEST: PROFIT WINDOW ACTIVATION
// ============================================================

#[test]
fn profit_window_activates() {
    let mut multi_sync = create_multi_sync();

    let mut regime = MarketRegime::MeanReversion;

    multi_sync
        .on_trade_closed(
            2.0,
            100.0,
            0,
            10_000,
            &mut regime,
        )
        .unwrap();

    assert!(multi_sync.is_window_active(10_000));

    assert_eq!(
        regime,
        MarketRegime::Trending,
    );

    assert_eq!(
        multi_sync.state.profit_protection.window_triggers,
        1,
    );
}


// ============================================================
// TEST: SCHEDULED REGIME RESTORATION
// ============================================================

#[test]
fn regime_restores_at_scheduled_expiration() {
    let mut multi_sync = create_multi_sync();

    let mut regime = MarketRegime::MeanReversion;

    multi_sync
        .on_trade_closed(
            2.0,
            100.0,
            0,
            10_000,
            &mut regime,
        )
        .unwrap();

    // Window expires at 100,000 ms.

    multi_sync.on_timer_tick(
        99_999,
        &mut regime,
    );

    assert_eq!(
        regime,
        MarketRegime::Trending,
    );

    multi_sync.on_timer_tick(
        100_000,
        &mut regime,
    );

    assert_eq!(
        regime,
        MarketRegime::MeanReversion,
    );

    // Accounting is still pending.

    assert!(multi_sync.has_pending_window());
}


// ============================================================
// TEST: ACCOUNTING COMPLETION
// ============================================================

#[test]
fn accounting_completes_on_trade_closed() {
    let mut multi_sync = create_multi_sync();

    let mut regime = MarketRegime::MeanReversion;

    // Trigger Profit Protection.

    multi_sync
        .on_trade_closed(
            2.0,
            100.0,
            0,
            10_000,
            &mut regime,
        )
        .unwrap();

    // Close another trade after the scheduled expiration.

    multi_sync
        .on_trade_closed(
            3.0,
            100.0,
            20_000,
            110_000,
            &mut regime,
        )
        .unwrap();

    // Both trades contributed to cumulative PnL.

    assert_eq!(
        multi_sync.state.running_realized_pnl,
        5.0,
    );

    // Anchor includes the expiration trade's PnL.

    assert_eq!(
        multi_sync.state.anchor_realized_pnl,
        5.0,
    );

    assert!(!multi_sync.has_pending_window());

    assert_eq!(
        regime,
        MarketRegime::MeanReversion,
    );
}


// ============================================================
// TEST: WINDOW TRADE CANNOT RETRIGGER
// ============================================================

#[test]
fn window_trade_cannot_retrigger() {
    let mut multi_sync = create_multi_sync();

    let mut regime = MarketRegime::MeanReversion;

    // First trade triggers Profit Protection.

    multi_sync
        .on_trade_closed(
            2.0,
            100.0,
            0,
            10_000,
            &mut regime,
        )
        .unwrap();

    // Complete accounting after expiration.

    multi_sync
        .on_trade_closed(
            0.0,
            100.0,
            0,
            110_000,
            &mut regime,
        )
        .unwrap();

    assert!(!multi_sync.has_pending_window());

    // This trade opened during the original window.
    //
    // It closes after accounting completion with
    // enough PnL to cross another threshold.

    multi_sync
        .on_trade_closed(
            10.0,
            100.0,
            20_000,
            120_000,
            &mut regime,
        )
        .unwrap();

    // PnL accumulates.

    assert_eq!(
        multi_sync.state.running_realized_pnl,
        12.0,
    );

    // But the window trade cannot directly retrigger.

    assert_eq!(
        multi_sync.state.profit_protection.window_triggers,
        1,
    );

    assert!(!multi_sync.has_pending_window());
}


// ============================================================
// TEST: DRAWDOWN WINDOW ACTIVATION
// ============================================================

#[test]
fn drawdown_window_activates() {
    let mut multi_sync = create_multi_sync();

    let mut regime = MarketRegime::MeanReversion;

    multi_sync
        .on_trade_closed(
            -2.0,
            100.0,
            0,
            10_000,
            &mut regime,
        )
        .unwrap();

    assert!(multi_sync.is_window_active(10_000));

    assert_eq!(
        regime,
        MarketRegime::Trending,
    );

    assert_eq!(
        multi_sync.state.drawdown_protection.window_triggers,
        1,
    );
}