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
};

use super::backtest_state::{
    MeanReversionConfig,
};


// ============================================================
// RESEARCH V4 LIVE STRATEGY
// ============================================================

pub struct ResearchV4Strategy {
    pub enable_strategy: bool,
    pub debug_mode: bool,

    pub(crate) tx_engine: UnboundedSender<EngineEvent>,
    pub(crate) rx_view: watch::Receiver<Arc<EngineView>>,

    pub max_trades_allowed: usize,

    pub reference_margin_usd: f64,
    pub trade_amount_usd: f64,

    pub leverage: Option<f64>,

    pub mean_reversion_configs: Vec<MeanReversionConfig>,

    pub candle_history: CandleHistory,
}