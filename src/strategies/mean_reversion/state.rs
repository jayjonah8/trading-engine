use std::{
    collections::{
        HashMap,
        VecDeque,
    },
    sync::Arc,
    time::Instant,
};

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


#[allow(dead_code)]
pub struct MeanReversionStrategy {
    pub(crate) enable_strategy: bool,

    pub(crate) debug_mode: bool,

    pub tx_engine:
        UnboundedSender<EngineEvent>,

    pub rx_view:
        watch::Receiver<Arc<EngineView>>,

    // Live rolling price history.
    pub(crate) window_prices:
        HashMap<
            String,
            VecDeque<(Instant, f64)>,
        >,

    pub(crate) entry_window_secs: u64,

    pub(crate) entry_percent: f64,

    // Closed multi-timeframe candle history.
    pub candle_history:
        CandleHistory,
}