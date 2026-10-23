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

    toolbox::black_swan_protection::BlackSwanProtection,

    shared::print_log,
};

use super::backtest_state::ResearchV2BacktestStrategy;

/*
TODO:

* You should check how much the asset is moving during the day (atr) for better profit targets per asset.

* To accumulate more, you will likely have to use limit orders that continues to grow you qty in that assets when it
  finally moves, but you will have to delete those limit orders when that assets is no longer in a trade.
* Each higher/lower limit order should have different stoploss parameters for ex. every added limit orders should have a
  tigher stop loose and possible lower trade duration if used. 
* - Or you could just fully accumulate during the side ways moves before the assets moves in any direction. Maybe have a mode
    to choose between limit accumulation or side ways / flat accumulation!

* Don't trade when the entire market moves together rapidly (maybe, we'll see)
* Most of the market follows Bitcoin anyway, so maybe you only need to follow BTC, and go down to additional assets based off
  how much volume they have the higher being the better.

* May use differnt timeframes for you indicators when examining the candles (ie. 2hrs, 4hrs (best), 6hrs, 8hrs, 24hrs etc.)
* Try to develop a function that dynamically examines the candles / assets markets to predict the next direction move. 

* Add the trailing stop loss logic that can be highly customized with different arbitrary dynamic levels.

* These trades will likely last around 2 - 8 hours. Will not use trade duration except for a 24hr duration
  incase a trade is stuct for some reason. Pare this with stale trades closes / messages. 


*/



// ============================================================
// STRATEGY DESCRIPTION
// ============================================================
//
// A predictive, position-accumulation strategy designed to
// identify directional opportunities during sideways markets
// and periods of price consolidation.
//
// Analyzes candle structures, price action, and asset-specific
// market behavior to anticipate breakouts and progressively
// accumulate positions before significant price movements occur.
//
// Includes specialized logic for high-momentum assets ("runners")
// exhibiting unusually large 24-hour price movements.
//
// Trade management will prioritize trailing stop-loss mechanisms
// to capture extended trends while protecting unrealized profits.
// Fixed trade durations will generally be avoided, with optional
// extended holding limits (e.g., 24 hours) as a safeguard.
//
// The objective is to maximize participation in directional
// price movements through early positioning and adaptive
// risk management.
//
// ============================================================
// CAPITAL DEPLOYMENT & PROFIT POTENTIAL
// ============================================================
//
// The strategy is designed for scalable capital deployment
// across a broad universe of independently traded assets.
//
// For example, allocating $10,000 per trade across 50 assets
// provides the potential to deploy $500,000 in total capital
// simultaneously, subject to available capital and risk limits.
//
// By accumulating positions before directional expansion,
// the strategy aims to capture larger price movements in full,
// such as a 5% gain, rather than entering after the move begins.
//
// A 5% profit target with a 2% stop-loss provides a theoretical
// reward-to-risk ratio of 2.5:1, allowing profitable trades
// to generate 2.5 times the amount risked per losing trade.
// ============================================================



impl ResearchV2BacktestStrategy {
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
            trade_amount_usd: 100.0,
            exit_profit_percent: Some(2.0),
            exit_loss_percent: Some(2.0),
            emergency_exit_percent: Some(5.0),
            min_sl_delay: Some(1800), // 30 minutes
            trade_duration_secs: Some(3_600), // 1 hour
            min_volume_usd: Some(450_000.0),
            max_24h_move_pct: None,

            candle_history: CandleHistory::new(
                candle_config.history_retention,
            ),
            can_trade: HashMap::new(),
            black_swan_protection: BlackSwanProtection::new(
                true,   // Enable protection.
                false,  // Enable or disable debug logging.
                15.0,   // Halt when average absolute 24-hour movement reaches 15%.
                8.0,    // Resume when average movement falls to 8%.
                15,     // Minimum number of assets required.
            ),
        }
    }
}


pub fn make_research_v2_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV2BacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


#[async_trait::async_trait]
impl Strategy for ResearchV2BacktestStrategy {

    fn name(&self) -> &'static str {
        "ResearchV2BacktestStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {

            AppEvent::TapeTrade(trade) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V2 BACKTEST][TAPE TRADE][{}] price={:.6} | ts_ms={}",
                            ev.symbol,
                            trade.price,
                            trade.timestamp_ms,
                        ),
                    );
                }
            }

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V2 BACKTEST][QUOTE][{}] bid={:.6} | ask={:.6} | ts_ms={}",
                            ev.symbol,
                            quote.bid_price,
                            quote.ask_price,
                            quote.timestamp_ms,
                        ),
                    );
                }
            }

            AppEvent::BarClosed(bar) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V2 BACKTEST][BAR CLOSED][{}] timeframe={:?} | close={:.6} | ts_ms={}",
                            ev.symbol,
                            bar.timeframe,
                            bar.close,
                            bar.end.timestamp_millis(),
                        ),
                    );
                }

                self.candle_history.push(bar);
            }
        }
    }
}