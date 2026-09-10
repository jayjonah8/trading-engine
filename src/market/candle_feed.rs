#![allow(dead_code)]

use chrono::{
    DateTime,
    Utc,
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::events::{
        AppEvent,
        AppEventEnvelope,
    },

    market::candle_manager::CandleManager,
};


// ============================================================
// CANDLE FEED ERROR
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandleFeedError {
    InvalidTimestamp,
    MasterChannelClosed,
}


// ============================================================
// PROCESS LIVE TAPE TRADE FOR CANDLES
// ============================================================

/// Feeds one canonical live tape trade into the shared `CandleManager`.
///
/// This helper is provider-independent and should be used by live market-data
/// providers such as Hyperliquid, Alpaca, IBKR, Databento, and future venues.
///
/// The venue's actual trade timestamp is used for candle placement. Any
/// completed candles are immediately published as canonical
/// `AppEvent::BarClosed` events through the master event channel.
///
/// IMPORTANT: Do NOT use this helper for historical/backtest feeds.
///
/// Historical backtests must build candles directly through `CandleManager`
/// from the complete raw trade stream, collect the completed bars, then merge
/// them with historical trade events and sort the combined replay timeline.
/// Publishing candles immediately through this helper would bypass that
/// deterministic historical replay ordering.
pub fn process_trade_for_candles(
    candle_manager: &mut CandleManager,
    tx_master: &UnboundedSender<AppEventEnvelope>,
    symbol: &str,
    timestamp_ms: i64,
    price: f64,
    size: f64,
) -> Result<(), CandleFeedError> {

    // ============================================================
    // CONVERT MARKET TIMESTAMP
    // ============================================================

    let Some(timestamp) = DateTime::<Utc>::from_timestamp_millis(timestamp_ms) else {
        return Err(CandleFeedError::InvalidTimestamp);
    };


    // ============================================================
    // UPDATE ALL CANDLE TIMEFRAMES
    // ============================================================

    // CandleManager closes completed candles first and then applies
    // this trade to the active candle for each configured timeframe.
    let closed_bars = candle_manager.on_trade(symbol, timestamp, price, size);


    // ============================================================
    // PUBLISH CLOSED CANDLES
    // ============================================================

    for bar in closed_bars {
        let symbol = bar.symbol.clone();

        let event = AppEventEnvelope {
            symbol,
            event: AppEvent::BarClosed(bar),
        };

        if tx_master.send(event).is_err() {
            return Err(CandleFeedError::MasterChannelClosed);
        }
    }


    Ok(())
}