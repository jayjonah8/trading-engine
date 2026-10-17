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

use super::state::ResearchV1Strategy;


// ============================================================
// CONSTRUCTOR
// ============================================================

impl ResearchV1Strategy {
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let candle_config = CandleConfig::default();

        Self {
            enable_strategy: true,
            debug_mode: true,
            tx_engine,
            rx_view,
            total_trade_events: 0,
            price_window: PriceWindowTs::new(
                10_800, // 3-hour rolling window
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

pub fn make_research_v1(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV1Strategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV1Strategy {
    fn name(&self) -> &'static str {
        "ResearchV1Strategy"
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
            // LIVE TRADE
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
                            "[RESEARCH V1 LIVE TRADE #{}][{}] \
                             price=${:.4} | size={} | timestamp_ms={}",
                            self.total_trade_events,
                            ev.symbol,
                            trade.price,
                            trade.size,
                            trade.timestamp_ms,
                        ),
                    );
                }
            }


            // ====================================================
            // LIVE QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {

                // ====================================================
                // MID PRICE
                // ====================================================

                let mid =
                    (quote.bid_price + quote.ask_price) / 2.0;

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V1 LIVE QUOTE][{}] \
                             bid=${:.4} | ask=${:.4} | mid=${:.4}",
                            ev.symbol,
                            quote.bid_price,
                            quote.ask_price,
                            mid,
                        ),
                    );
                }


                // ====================================================
                // TIMESTAMP-BASED PRICE WINDOW
                // ====================================================

                let result = self.price_window
                    .update_price_window_extreme_diffs_ts(
                        &ev.symbol,
                        quote.timestamp_ms,
                        mid,
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
                                "[RESEARCH V1 LIVE WINDOW][{}] \
                                 mid=${:.4} | \
                                 above_min={:.3}% | \
                                 below_max={:.3}% | \
                                 age={}s | \
                                 entry_threshold={:.3}%",
                                ev.symbol,
                                mid,
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
                                    "[RESEARCH V1 LIVE SIGNAL][{}] \
                                     mid=${:.4} | \
                                     above_min={:.3}% >= {:.3}%",
                                    ev.symbol,
                                    mid,
                                    pct_from_min,
                                    self.entry_percent,
                                ),
                            );
                        }
                    }
                }
            }


            // ====================================================
            // LIVE CANDLE
            // ====================================================

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;

                self.candle_history.push(bar);

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V1 LIVE CANDLE][{}][{}s] stored={}",
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