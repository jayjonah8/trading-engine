use crate::core::{
    engine::engine::EngineEvent,
    events::{
        AppEvent,
        AppEventEnvelope,
    },
};


// ============================================================
// LIVE ENGINE EVENT MAPPING
// ============================================================

/// Converts live market data into an engine price update.
///
/// Live risk management uses the midpoint between the current
/// best bid and ask as the engine's market price.
///
/// Tape-trade and completed-candle events do not generate
/// `EngineEvent::PriceTick` events.
pub(crate) fn map_app_to_live_engine_event(
    env: &AppEventEnvelope,
) -> Option<EngineEvent> {
    match &env.event {
        AppEvent::TapeTrade(_) => None,

        AppEvent::Quote(quote) => {
            let ts_ms = quote.timestamp_ms;

            if !quote.bid_price.is_finite()
                || !quote.ask_price.is_finite()
                || quote.bid_price <= 0.0
                || quote.ask_price <= 0.0
                || quote.ask_price < quote.bid_price
            {
                return None;
            }

            let mid = (quote.bid_price + quote.ask_price) * 0.5;

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