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

    toolbox::price_window_ts::PriceWindowTs,

    shared::print_log,
};


// ============================================================
// RESEARCH V1 BACKTEST STRATEGY
// ============================================================

pub struct ResearchV1BacktestStrategy {
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,

    pub enable_strategy: bool,
    pub debug_mode: bool,

    pub total_trade_events: u64,

    // Historical timestamp-based rolling price window.
    pub price_window: PriceWindowTs,

    // Entry threshold percentage.
    pub entry_percent: f64,

    // Closed multi-timeframe candle history.
    pub candle_history: CandleHistory,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl ResearchV1BacktestStrategy {
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let candle_config = CandleConfig::default();

        Self {
            tx_engine,
            rx_view,
            enable_strategy: true,
            debug_mode: true,
            total_trade_events: 0,
            price_window: PriceWindowTs::new(
                10_800, // 3-hour historical rolling window
                false,
            ),
            entry_percent: 0.3,
            candle_history: CandleHistory::new(
                candle_config.history_retention,
            ),
        }
    }
}


// ============================================================
// FACTORY
// ============================================================

pub fn make_research_v1_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV1BacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV1BacktestStrategy {
    fn name(&self) -> &'static str {
        "ResearchV1BacktestStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {

            // ====================================================
            // HISTORICAL TRADE
            // ====================================================

            AppEvent::TapeTrade(trade) => {
                self.total_trade_events += 1;

                // ====================================================
                // TRADE DEBUG
                // ====================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V1 BACKTEST TRADE #{}][{}] \
                             price=${:.4} | size={} | timestamp_ms={}",
                            self.total_trade_events,
                            ev.symbol,
                            trade.price,
                            trade.size,
                            trade.timestamp_ms,
                        ),
                    );
                }


                // ====================================================
                // HISTORICAL PRICE WINDOW
                // ====================================================

                let result = self.price_window
                    .update_price_window_extreme_diffs_ts(
                        &ev.symbol,
                        trade.timestamp_ms,
                        trade.price,
                    );

                if let Some((
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                )) = result {
                    if self.debug_mode {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[RESEARCH V1 BACKTEST WINDOW][{}] \
                                 price=${:.4} | \
                                 above_min={:.3}% | \
                                 below_max={:.3}% | \
                                 age={}s | \
                                 entry_threshold={:.3}%",
                                ev.symbol,
                                trade.price,
                                pct_from_min,
                                pct_from_max,
                                elapsed_secs,
                                self.entry_percent,
                            ),
                        );
                    }

                    // ====================================================
                    // ENTRY SIGNAL
                    // ====================================================

                    // Research-only signal detection.
                    // No orders are submitted yet.
                    if pct_from_min >= self.entry_percent {
                        if self.debug_mode {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[RESEARCH V1 BACKTEST SIGNAL][{}] \
                                     price=${:.4} | \
                                     above_min={:.3}% >= {:.3}%",
                                    ev.symbol,
                                    trade.price,
                                    pct_from_min,
                                    self.entry_percent,
                                ),
                            );
                        }
                    }
                }
            }


            // ====================================================
            // HISTORICAL QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V1 BACKTEST QUOTE][{}] \
                             bid=${:.4} | ask=${:.4}",
                            ev.symbol,
                            quote.bid_price,
                            quote.ask_price,
                        ),
                    );
                }
            }


            // ====================================================
            // HISTORICAL CANDLE
            // ====================================================

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;

                self.candle_history.push(bar);

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V1 BACKTEST CANDLE][{}][{}s] stored={}",
                            ev.symbol,
                            timeframe.as_secs(),
                            self.candle_history.len(
                                &ev.symbol,
                                timeframe,
                            ),
                        ),
                    );
                }
            }
        }
    }
}