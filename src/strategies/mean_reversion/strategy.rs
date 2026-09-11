use std::{
    collections::HashMap,
    sync::Arc,
    time::Instant,
};

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::{
    core::{
        engine::EngineEvent,
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

use super::state::
    MeanReversionStrategy;


impl MeanReversionStrategy {
    pub fn new(
        tx_engine:
            UnboundedSender<EngineEvent>,

        rx_view:
            watch::Receiver<
                Arc<EngineView>
            >,
    ) -> Self {
        let candle_config =
            CandleConfig::default();

        Self {
            enable_strategy: false,

            debug_mode: true,

            tx_engine,

            rx_view,

            window_prices:
                HashMap::new(),

            entry_window_secs:
                1500,

            entry_percent:
                2.0,

            candle_history:
                CandleHistory::new(
                    candle_config
                        .history_retention
                ),
        }
    }
}


pub fn make_mean_reversion(
    tx_engine:
        UnboundedSender<EngineEvent>,

    rx_view:
        watch::Receiver<
            Arc<EngineView>
        >,
) -> Box<dyn Strategy> {
    Box::new(
        MeanReversionStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


#[async_trait::async_trait]
impl Strategy
    for MeanReversionStrategy
{
    fn name(
        &self,
    ) -> &'static str {
        "MeanReversionStrategy"
    }


    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }


        match ev.event {

            // ================================================
            // LIVE TRADE
            // ================================================

            AppEvent::Trade(trade) => {
                if self.debug_mode {
                    println!(
                        "[MEAN REVERSION LIVE TRADE][{}] \
                         price=${:.4} | size={}",
                        ev.symbol,
                        trade.price,
                        trade.size,
                    );
                }
            }


            // ================================================
            // LIVE QUOTE
            // ================================================

            AppEvent::Quote(quote) => {
                let mid =
                    (
                        quote.bid_price
                            + quote.ask_price
                    )
                    / 2.0;


                if let Some((
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                )) =
                    self
                        .update_and_get_window_extreme_diffs(
                            &ev.symbol,
                            Instant::now(),
                            mid,
                        )
                {
                    if self.debug_mode {
                        println!(
                            "[MEAN REVERSION LIVE WINDOW][{}] \
                             mid=${:.4} | \
                             above_min={:.3}% | \
                             below_max={:.3}% | \
                             age={}s",
                            ev.symbol,
                            mid,
                            pct_from_min,
                            pct_from_max,
                            elapsed_secs,
                        );
                    }
                }
            }


            // ================================================
            // LIVE CANDLE
            // ================================================

            AppEvent::BarClosed(
                bar
            ) => {
                let timeframe =
                    bar.timeframe;

                self.candle_history
                    .push(bar);

                if self.debug_mode {
                    println!(
                        "[MEAN REVERSION LIVE CANDLE][{}][{}s] stored={}",
                        ev.symbol,
                        timeframe.as_secs(),
                        self.candle_history.len(
                            &ev.symbol,
                            timeframe,
                        ),
                    );
                }
            }
        }
    }
}