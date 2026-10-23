#![allow(dead_code)]

mod base;
mod indicators;
mod limit_order;
mod limit_order_advanced;
mod market_order;
mod mean_reversion;
mod research_v1;
mod research_v2;

pub use base::BaseStrategy;
pub use limit_order::LimitOrderStrategy;
pub use limit_order_advanced::LimitOrderAdvancedStrategy;
pub use market_order::MarketOrderStrategy;

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
    Indicator,
    MeanReversion,
    ResearchV1,
    ResearchV2,
    MarketOrder,
    LimitOrder,
    LimitOrderAdvanced,
}


#[async_trait::async_trait]
pub trait Strategy: Send {
    fn name(&self) -> &'static str;

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    );
}


// ============================================================
// LIVE / SIMULATION STRATEGY FACTORY
// ============================================================

pub fn make_strategy(
    kind: &StrategyKind,
    tx_engine: TokioUnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
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

        StrategyKind::Indicator => {
            indicators::make_indicator_strategy(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::MeanReversion => {
            mean_reversion::make_mean_reversion(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::ResearchV1 => {
            research_v1::make_research_v1(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::ResearchV2 => {
            research_v2::make_research_v2(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::MarketOrder => {
            Box::new(
                MarketOrderStrategy::new(
                    tx_engine,
                    rx_view,
                )
            )
        }

        StrategyKind::LimitOrder => {
            Box::new(
                LimitOrderStrategy::new(
                    tx_engine,
                    rx_view,
                )
            )
        }

        StrategyKind::LimitOrderAdvanced => {
            Box::new(
                LimitOrderAdvancedStrategy::new(
                    tx_engine,
                    rx_view,
                )
            )
        }
    }
}


// ============================================================
// BACKTEST STRATEGY FACTORY
// ============================================================

pub fn make_backtest_strategy(
    kind: &StrategyKind,
    tx_engine: TokioUnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    match kind {
        StrategyKind::Base => {
            base::make_base_backtest(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::Indicator => {
            indicators::make_indicator_backtest(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::MeanReversion => {
            mean_reversion::make_mean_reversion_backtest(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::ResearchV1 => {
            research_v1::make_research_v1_backtest(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::ResearchV2 => {
            research_v2::make_research_v2_backtest(
                tx_engine,
                rx_view,
            )
        }

        StrategyKind::MarketOrder => {
            panic!(
                "MarketOrderStrategy is a live execution test strategy and cannot run in backtest mode"
            );
        }

        StrategyKind::LimitOrder => {
            panic!(
                "LimitOrderStrategy is a live execution test strategy and cannot run in backtest mode"
            );
        }

        StrategyKind::LimitOrderAdvanced => {
            panic!(
                "LimitOrderAdvancedStrategy is a live execution test strategy and cannot run in backtest mode"
            );
        }
    }
}