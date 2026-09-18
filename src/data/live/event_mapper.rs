use crate::core::{
    engine::engine::EngineEvent,
    events::{
        AppEvent,
        AppEventEnvelope,
    },
};


// ============================================================
// APP EVENT -> ENGINE EVENT MAPPING
// ============================================================
//
// `AppEvent` and `EngineEvent` serve different purposes.
//
// Market-data events published through `tx_master` are delivered directly
// to strategies regardless of whether this mapper produces an EngineEvent.
//
// For example:
//
//     AppEvent::TapeTrade
//         ├──> CandleManager
//         ├──> Strategies
//         └──> Engine mapper -> None
//
// Returning `None` for `TapeTrade` does NOT prevent strategies from receiving
// or using tape trades. It only means the tape trade does not generate an
// `EngineEvent::PriceTick` or otherwise directly update the Engine.
//
// The current live engine price is driven by `Quote` events using the
// midpoint between the best bid and ask:
//
//     AppEvent::Quote
//         ├──> Strategies
//         └──> Engine mapper
//                  └──> EngineEvent::PriceTick
//
// This allows strategies to independently use trades, quotes, and bars while
// keeping the Engine's authoritative price-update behavior explicitly defined
// by this mapper.
//
//
// ENGINE MARKET DATA
//         │
//         ├───────────────┐
//         │               │
//         ▼               ▼
//    TAPE TRADES        QUOTES
//    actual trades      bid / ask
//         │               │
//         │               ├──→ BaseStrategy trading logic
//         │               │       ↓
//         │               │    midpoint
//         │               │       ↓
//         │               │    entry signals
//         │               │
//         │               └──→ Engine PriceTick
//         │
//         ├──→ CandleManager
//         │       ↓
//         │    OHLCV bars
//         │       ↓
//         │    BarClosed
//         │
//         └──→ BaseStrategy
//                 ↓
//              log only
//
// Tape trades = candle construction.
// Quotes = live trading decisions + Engine current-price updates.


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