use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use chrono::Utc;

use tokio::sync::{
    mpsc::UnboundedSender as TokioUnboundedSender,
    watch,
};

use crate::core::{
    engine::EngineEvent,
    engine_view::EngineView,
    trade_intent::TradeIntent,
    events::{
        AppEvent,
        AppEventEnvelope,
    },
};

use crate::market::{
    candle::CandleConfig,
    candle_history::CandleHistory,
};

use crate::strategies::Strategy;

use super::state::BaseStrategy;
use crate::shared::print_log;

use crate::market::session::regular_market_session;
use crate::strategies::base::strategy_utils::send_base_strategy_start_message;

use crate::strategies::base::strategy_session::{
    strategy_trading_window,
};

impl BaseStrategy {
    pub fn new(
        tx_engine: TokioUnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {

        let candle_config = CandleConfig::default();

        let strategy = Self {
            enable_strategy: true,
            debug_mode: true,
            tx_engine,
            rx_view,
            window_prices: HashMap::new(),
            entry_window_secs: 1500,
            trade_amount_usd: 100.0,
            entry_percent: 2.0,
            exit_profit_percent: 2.0,
            exit_loss_percent: 2.0,
            emergency_exit_percent: 5.0,
            min_sl_delay: 30,
            enable_trade_duration: true,
            trade_duration_secs: Some(3600),
            track_price_window_while_in_trade: false,
            candle_history: CandleHistory::new(
                candle_config.history_retention
            ),
            market_open_delay_minutes: 15,
            market_close_buffer_minutes: 15,
            close_all_before_market_close_minutes: 10,
            close_all_sent_for_session: None,
            track_prices_during_open_delay: false,
            current_session_start_by_symbol: HashMap::new(),
        };

        send_base_strategy_start_message(&strategy);

        strategy
    }
}

pub fn make_base(
    tx_engine: TokioUnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        BaseStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}

#[async_trait::async_trait]
impl Strategy for BaseStrategy {
    fn name(&self) -> &'static str {
        "BaseStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {
            AppEvent::Trade(trade) => {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[BaseStrategy][{}] TRADE | price=${:.2} | size={} | {}",
                        ev.symbol,
                        trade.price,
                        trade.size,
                        trade.timestamp,
                    ),
                );
            }

            AppEvent::Quote(quote) => {

                // Live entry flow:
                // Regular-hours filter
                // → Reset state for a new market session
                // → Calculate the strategy trading window
                // → Optionally track prices during the opening delay
                // → Optionally stop tracking prices while already in a trade
                // → Update the rolling price window
                // → Check whether entries are currently allowed
                // → Check the latest EngineView state
                // → Send a live short or long trade intent

                // Use one wall-clock timestamp for all market-session
                // decisions made for this quote.
                let timestamp = Utc::now();

                // Use the midpoint between the current best bid and ask
                // as the strategy's current market price.
                let mid =
                    (quote.bid_price + quote.ask_price) / 2.0;


                // ============================================================
                // REGULAR MARKET SESSION
                // ============================================================

                let Some((
                    session_start,
                    session_end,
                )) = regular_market_session(
                    timestamp
                ) else {
                    return;
                };

                // Ignore all premarket and after-hours quotes.
                if timestamp < session_start
                    || timestamp >= session_end
                {
                    return;
                }


                // ============================================================
                // NEW SESSION RESET
                // ============================================================

                let previous_session =
                    self.current_session_start_by_symbol
                        .get(&ev.symbol)
                        .copied();

                if previous_session
                    != Some(session_start)
                {
                    // Remove all rolling prices belonging to the
                    // previous trading session for this symbol.
                    self.window_prices
                        .remove(&ev.symbol);

                    self.current_session_start_by_symbol
                        .insert(
                            ev.symbol.clone(),
                            session_start,
                        );

                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[BASE LIVE SESSION RESET][{}] session_start={}",
                            ev.symbol,
                            session_start,
                        ),
                    );
                }


                // ============================================================
                // STRATEGY TRADING WINDOW
                // ============================================================

                let Some((
                    trading_start,
                    trading_end,
                )) = strategy_trading_window(
                    timestamp,
                    self.market_open_delay_minutes,
                    self.market_close_buffer_minutes,
                ) else {
                    return;
                };


                // ============================================================
                // END-OF-SESSION FORCE CLOSE
                // ============================================================

                // The force-close time is independent from trading_end.
                //
                // market_close_buffer_minutes:
                //     Controls when NEW trade entries stop.
                //
                // close_all_before_market_close_minutes:
                //     Controls when any remaining open trades are force-closed.
                //
                // Example:
                //
                //     Market close = 4:00 PM
                //
                //     market_close_buffer_minutes = 15
                //         → Stop new entries at 3:45 PM
                //
                //     close_all_before_market_close_minutes = 10
                //         → Force-close remaining trades at 3:50 PM
                let close_all_time =
                    session_end
                        - chrono::Duration::minutes(
                            self.close_all_before_market_close_minutes
                        );


                // Only send one global close-all request for each market session.
                //
                // Because CloseAllTrades closes every open trade in the engine,
                // this state is tracked once for the strategy rather than per symbol.
                if timestamp >= close_all_time {
                    if self.close_all_sent_for_session
                        != Some(session_start)
                    {
                        self.close_all_sent_for_session =
                            Some(session_start);

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[BASE LIVE FORCE CLOSE ALL] \
                                now={} | \
                                close_all_time={} | \
                                session_end={} | \
                                close_before_market_close={}min",
                                timestamp,
                                close_all_time,
                                session_end,
                                self.close_all_before_market_close_minutes,
                            ),
                        );

                        let _ =
                            self.tx_engine.send(
                                EngineEvent::CloseAllTrades {
                                    reason: format!(
                                        "Forced close {} minutes before market close",
                                        self.close_all_before_market_close_minutes,
                                    ),
                                }
                            );
                    }

                    // Do not perform any more strategy work once the force-close
                    // period for this market session has been reached.
                    return;
                }


                // ============================================================
                // OPENING DELAY PRICE TRACKING
                // ============================================================

                // When disabled, completely ignore prices during the
                // opening delay instead of adding them to the rolling window.
                if !self.track_prices_during_open_delay
                    && timestamp < trading_start
                {
                    return;
                }

                // ============================================================
                // ENGINE VIEW
                // ============================================================

                let view: Arc<EngineView> =
                    self.rx_view.borrow().clone();

                    
                // ============================================================
                // OPEN TRADE PRICE-WINDOW TRACKING
                // ============================================================

                // When disabled, stop tracking this symbol's rolling price
                // window while it already has an open trade.
                if !self.track_price_window_while_in_trade
                    && view.trades
                        .has_open_trade_for_symbol(&ev.symbol)
                {
                    if self.window_prices
                        .remove(&ev.symbol)
                        .is_some()
                    {
                        print_log(
                            self.debug_mode,
                            false,
                            format!(
                                "[BASE LIVE WINDOW CLEARED][{}] \
                                open trade detected; price window cleared",
                                ev.symbol,
                            ),
                        );
                    }

                    return;
                }
 

                // ============================================================
                // ROLLING PRICE WINDOW
                // ============================================================

                if let Some((
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                )) =
                    self.update_and_get_window_extreme_diffs(
                        &ev.symbol,
                        Instant::now(),
                        mid,
                    )
                {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[BASE LIVE WINDOW][{}] \
                            price=${:.4} | \
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


                    // ============================================================
                    // ENTRY TIME GATE
                    // ============================================================

                    // Prices may be collected during the opening delay,
                    // but entries cannot be sent until trading_start.
                    if timestamp < trading_start {
                        return;
                    }

                    // Stop generating new entries before the market closes.
                    if timestamp >= trading_end {
                        return;
                    }


                    // ============================================================
                    // EXISTING OPEN TRADE GATE
                    // ============================================================

                    // Do not generate another intent when this symbol
                    // already has an open position.
                    if view.trades
                        .has_open_trade_for_symbol(
                            &ev.symbol
                        )
                    {
                        return;
                    }


                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[BASE LIVE TRADING WINDOW][{}] \
                            now={} | \
                            trading_start={} | \
                            trading_end={}",
                            ev.symbol,
                            timestamp,
                            trading_start,
                            trading_end,
                        ),
                    );


                    // ============================================================
                    // SHORT ENTRY
                    // ============================================================

                    // Price has risen by the configured percentage from
                    // the rolling low. Enter short expecting mean reversion.
                    if pct_from_min >= self.entry_percent {
                        let intent = TradeIntent {
                            strategy: self.name().to_string(),
                            symbol: ev.symbol.clone(),
                            is_long: false,
                            price: mid,
                            trade_amount_usd: self.trade_amount_usd,
                            entry_percent: pct_from_min,
                            exit_profit_percent: self.exit_profit_percent,
                            exit_loss_percent: self.exit_loss_percent,
                            emergency_exit_percent: self.emergency_exit_percent,
                            min_sl_delay: self.min_sl_delay,
                            enable_trade_duration: self.enable_trade_duration,
                            trade_duration_secs: self.trade_duration_secs,
                            ts_ms: timestamp.timestamp_millis(),
                        };

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[BASE LIVE SHORT ENTRY][{}] \
                                price=${:.4} | \
                                above_min={:.3}% >= {:.3}% | \
                                amount=${:.2}",
                                ev.symbol,
                                mid,
                                pct_from_min,
                                self.entry_percent,
                                self.trade_amount_usd,
                            ),
                        );

                        let _ = self.tx_engine.send(
                            EngineEvent::OpenMarketOrder(
                                intent
                            )
                        );
                    }


                    // ============================================================
                    // LONG ENTRY
                    // ============================================================

                    // Price has fallen by the configured percentage from
                    // the rolling high. Enter long expecting mean reversion.
                    else if pct_from_max >= self.entry_percent {
                        let intent = TradeIntent {
                            strategy: self.name().to_string(),
                            symbol: ev.symbol.clone(),
                            is_long: true,
                            price: mid,
                            trade_amount_usd: self.trade_amount_usd,
                            entry_percent: pct_from_max,
                            exit_profit_percent: self.exit_profit_percent,
                            exit_loss_percent: self.exit_loss_percent,
                            emergency_exit_percent: self.emergency_exit_percent,
                            min_sl_delay: self.min_sl_delay,
                            enable_trade_duration: self.enable_trade_duration,
                            trade_duration_secs: self.trade_duration_secs,
                            ts_ms: timestamp.timestamp_millis(),
                        };

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[BASE LIVE LONG ENTRY][{}] \
                                price=${:.4} | \
                                below_max={:.3}% >= {:.3}% | \
                                amount=${:.2}",
                                ev.symbol,
                                mid,
                                pct_from_max,
                                self.entry_percent,
                                self.trade_amount_usd,
                            ),
                        );

                        let _ = self.tx_engine.send(
                            EngineEvent::OpenMarketOrder(
                                intent
                            )
                        );
                    }
                }


                // ============================================================
                // QUOTE LOG
                // ============================================================

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[BaseStrategy] symbol={} | \
                        mid=${:.2} | \
                        bid=${:.2} ({}) | \
                        ask=${:.2} ({})",
                        ev.symbol,
                        mid,
                        quote.bid_price,
                        quote.bid_size,
                        quote.ask_price,
                        quote.ask_size,
                    ),
                );
            }

            
            AppEvent::BarClosed(bar) => {
                // Candle history may not contain enough data yet.
                // Always check for None / required length before using candles.
                // If data is missing, safely skip the trading decision.

                let timeframe =
                    bar.timeframe;

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[BaseStrategy][{}][{}s] BAR CLOSED | \
                         O={:.2} H={:.2} L={:.2} C={:.2} V={}",
                        ev.symbol,
                        timeframe.as_secs(),
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                    ),
                );

                // Store the newly completed candle in the strategy's candle history.
                self.candle_history
                    .push(bar);
            }
        }
    }
}