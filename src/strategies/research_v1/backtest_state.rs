#![allow(dead_code)]

use std::sync::Arc;

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::{
    core::{
        engine::engine::EngineEvent,
        engine_view::EngineView,
    },

    market::candle_history::CandleHistory,

    toolbox::price_window_ts::PriceWindowTs,
};


// ============================================================
// RESEARCH V1 BACKTEST STRATEGY
// ============================================================

pub struct ResearchV1BacktestStrategy {
    // Master switch to enable or disable the strategy.
    pub enable_strategy: bool,

    // Master switch to enable or disable strategy debug logging.
    pub debug_mode: bool,

    // Channel used to send strategy events and trade requests to the engine.
    pub(crate) tx_engine: UnboundedSender<EngineEvent>,

    // Receiver providing the latest shared engine state.
    pub(crate) rx_view: watch::Receiver<Arc<EngineView>>,

    // Amount of USD allocated to each trade before leverage (e.g. 100.0 = $100)
    pub trade_amount_usd: f64,

    // Percentage move from a window extreme required to trigger an entry (e.g. 2.0 = 2%)
    pub entry_percent: f64,

    // Profit percentage at which the position should be closed (e.g. 3.0 = 3%).
    pub exit_profit_percent: f64,

    // Loss percentage at which the position should be closed.
    pub exit_loss_percent: f64,

    // Emergency stop-loss.
    pub emergency_exit_percent: f64,

    // Duration in seconds to delay hard stop-loss.
    pub min_sl_delay: u64,

    // Mode to limit how long a trade can be open.
    pub enable_trade_duration: bool,

    // Time limit in seconds until an open trade is auto closed.
    pub trade_duration_secs: Option<u64>,

    // Minimum USD trading volume required to allow a trade entry.
    // For a rolling-volume filter, accumulate this value over a defined time window rather than indefinitely.
    pub min_volume_usd: f64,

    // Maximum allowed 24-hour price movement percentage.
    // None = no limit.
    pub max_24h_move_pct: Option<f64>,

    // true  = keep tracking the symbol's rolling price window while a trade is open.
    // false = clear that symbol's rolling window and stop tracking it until the trade is closed.
    pub track_price_window_while_in_trade: bool,

    // Historical timestamp-based rolling price window.
    pub price_window: PriceWindowTs,

    // Closed multi-timeframe candle history.
    pub candle_history: CandleHistory,
}