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
// RESEARCH V1 STRATEGY STATE
// ============================================================

#[allow(dead_code)]
pub struct ResearchV1Strategy {
    pub(crate) enable_strategy: bool,
    pub(crate) debug_mode: bool,

    pub tx_engine: UnboundedSender<EngineEvent>,
    pub rx_view: watch::Receiver<Arc<EngineView>>,

    // Total market trade events processed.
    pub total_trade_events: u64,

    // Timestamp-based rolling price window.
    pub price_window: PriceWindowTs,

    // Entry threshold percentage.
    pub entry_percent: f64,

    // Closed multi-timeframe candle history.
    pub candle_history: CandleHistory,
}