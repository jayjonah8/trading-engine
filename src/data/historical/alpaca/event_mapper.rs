#![allow(dead_code)]

use crate::core::{
    engine::engine::EngineEvent,
    events::{
        AppEvent,
        AppEventEnvelope,
    },
};

// ============================================================
// Backtest Engine Event Mapping
// ============================================================

/// Converts historical market data into a deterministic
/// backtest price event.
///
/// Historical trades preserve their original timestamp so risk
/// calculations remain tied to replay time instead of wall time.
pub(crate) fn map_app_to_backtest_engine_event(
    env: &AppEventEnvelope,
) -> Option<EngineEvent> {
    match &env.event {
        AppEvent::TapeTrade(trade) => {
            let ts_ms = trade.timestamp_ms;

            if !trade.price.is_finite() || trade.price <= 0.0 {
                return None;
            }

            Some(
                EngineEvent::PriceTickBacktest {
                    symbol: env.symbol.clone(),
                    px: trade.price,
                    ts_ms,
                }
            )
        }

        // Historical backtests currently replay trades
        // rather than quote data.
        AppEvent::Quote(_) => None,

        // Candle closes are not used as risk prices because the
        // historical trade stream already provides the price path.
        AppEvent::BarClosed(_) => None,
    }
}