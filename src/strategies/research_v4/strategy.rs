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
        events::{
            AppEvent,
            AppEventEnvelope,
        },
    },

    market::{
        candle::CandleConfig,
        candle_history::CandleHistory,
    },

    strategies::Strategy,
};

use super::state::ResearchV4Strategy;


// ============================================================
// CONSTRUCTOR
// ============================================================

impl ResearchV4Strategy {
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let candle_config = CandleConfig::default();

        Self {
            enable_strategy: false,
            debug_mode: false,

            tx_engine,
            rx_view,

            max_trades_allowed: 8,

            reference_margin_usd: 1_600.0,
            trade_amount_usd: 200.0,

            leverage: None,

            trending_configs: Vec::new(),
            mean_reversion_configs: Vec::new(),

            candle_history: CandleHistory::new(
                candle_config.history_retention,
            ),
        }
    }
}


// ============================================================
// FACTORY
// ============================================================

pub fn make_research_v4(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV4Strategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV4Strategy {
    fn name(&self) -> &'static str {
        "ResearchV4Strategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {
            AppEvent::TapeTrade(_) => {
                // TODO: Live trade processing.
            }

            AppEvent::Quote(_) => {
                // TODO: Live quote processing.
            }

            AppEvent::BarClosed(bar) => {
                self.candle_history.push(bar);

                // TODO: Live indicator and setup processing.
            }
        }
    }
}