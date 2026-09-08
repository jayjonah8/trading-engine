use chrono::DateTime;

use crate::core::{
    engine::EngineEvent,
    events::{
        AppEvent,
        AppEventEnvelope,
    },
};


// ============================================================
// Live Engine Event Mapping
// ============================================================

/// Converts live market data into an engine price update.
///
/// Live risk management uses the midpoint between the current
/// best bid and ask as the engine's market price.
///
/// Trade and completed-candle events do not generate PriceTick events.
pub(crate) fn map_app_to_live_engine_event(
    env: &AppEventEnvelope,
) -> Option<EngineEvent> {
    match &env.event {
        AppEvent::Trade(_) => None,

        AppEvent::Quote(quote) => {
            let ts_ms = DateTime::parse_from_rfc3339(
                &quote.timestamp
            )
            .ok()?
            .timestamp_millis();

            if !quote.bid_price.is_finite()
                || !quote.ask_price.is_finite()
                || quote.bid_price <= 0.0
                || quote.ask_price <= 0.0
                || quote.ask_price < quote.bid_price
            {
                return None;
            }

            let mid =
                (quote.bid_price + quote.ask_price) * 0.5;

            Some(
                EngineEvent::PriceTick {
                    symbol: env.symbol.clone(),
                    px: mid,
                    ts_ms,
                }
            )
        }

        AppEvent::BarClosed(_) => None,
    }
}


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
        AppEvent::Trade(trade) => {
            let ts_ms = DateTime::parse_from_rfc3339(
                &trade.timestamp
            )
            .ok()?
            .timestamp_millis();

            if !trade.price.is_finite()
                || trade.price <= 0.0
            {
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