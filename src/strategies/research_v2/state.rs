#![allow(dead_code)]

use std::sync::Arc;
use std::collections::HashMap;

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

    toolbox::black_swan_protection::BlackSwanProtection,
};


pub struct ResearchV2Strategy {
    // Master switch to enable or disable the strategy.
    pub enable_strategy: bool,

    // Master switch to enable or disable strategy debug logging.
    pub debug_mode: bool,

    // Channel used to send strategy events and trade requests to the engine.
    pub(crate) tx_engine: UnboundedSender<EngineEvent>,

    // Receiver providing the latest shared engine state.
    pub(crate) rx_view: watch::Receiver<Arc<EngineView>>,

    // Amount of USD allocated to each trade before leverage.
    pub trade_amount_usd: f64,

    // Profit percentage at which the position should be closed.
    pub exit_profit_percent: Option<f64>,

    // Loss percentage at which the position should be closed.
    pub exit_loss_percent: Option<f64>,

    // Emergency stop-loss.
    pub emergency_exit_percent: Option<f64>,

    // Duration in seconds to delay hard stop-loss.
    pub min_sl_delay: Option<u64>,

    // Time limit in seconds until an open trade is automatically closed.
    pub trade_duration_secs: Option<u64>,

    // Minimum USD trading volume required to allow a trade entry.
    pub min_volume_usd: Option<f64>,

    // Maximum allowed 24-hour price movement percentage.
    // None = no limit.
    pub max_24h_move_pct: Option<f64>,

    // Closed multi-timeframe candle history.
    pub candle_history: CandleHistory,

    // Cached trading eligibility for each symbol.
    pub can_trade: HashMap<String, bool>,

    // Market-wide black swan protection.
    pub black_swan_protection: BlackSwanProtection,
}