#![allow(dead_code)]

mod base;
mod mean_reversion;

pub use base::BaseStrategy;

use std::sync::Arc;

use tokio::sync::{
    mpsc::UnboundedSender as TokioUnboundedSender,
    watch,
};

use crate::core::{
    engine::engine::EngineEvent,
    engine_view::EngineView,
    events::AppEventEnvelope,
};
#[derive(Clone, Debug)]
pub enum StrategyKind {
    Base,
    MeanReversion,
}

#[async_trait::async_trait]
pub trait Strategy: Send {
    fn name(&self) -> &'static str;

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    );
}

pub fn make_strategy(
    kind: &StrategyKind,
    tx_engine:
        TokioUnboundedSender<EngineEvent>,
    rx_view:
        watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    match kind {

        StrategyKind::Base => {
            Box::new(
                BaseStrategy::new(
                    tx_engine,
                    rx_view,
                )
            )
        }

        StrategyKind::MeanReversion => {
            mean_reversion::
                make_mean_reversion(
                    tx_engine,
                    rx_view,
                )
        }
    }
}

pub fn make_backtest_strategy(
    kind: &StrategyKind,
    tx_engine:
        TokioUnboundedSender<EngineEvent>,
    rx_view:
        watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    match kind {
        StrategyKind::Base => {
            base::make_base_backtest(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::MeanReversion => {
            mean_reversion::
                make_mean_reversion_backtest(
                    tx_engine,
                    rx_view,
                )
        }
    }
}