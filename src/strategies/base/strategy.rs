#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
    time::Instant,
};

use chrono::{
    DateTime,
    Utc,
};

use tokio::sync::{
    mpsc::UnboundedSender as TokioUnboundedSender,
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
        execution::order::{
            OrderType,
            TimeInForce,
        },
        trade_intent::TradeIntent,
        trades::Direction,
    },

    market::{
        candle::CandleConfig,
        candle_history::CandleHistory,
        session::regular_market_session,
    },

    shared::print_log,

    strategies::{
        base::{
            state::BaseStrategy,
            strategy_session::strategy_trading_window,
            strategy_utils::send_base_strategy_start_message,
        },
        Strategy,
    },
};


// ============================================================
// BASE STRATEGY
// ============================================================

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
            candle_history: CandleHistory::new(candle_config.history_retention),
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


// ============================================================
// STRATEGY FACTORY
// ============================================================

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


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

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

            // ========================================================
            // TAPE TRADE
            // ========================================================

            AppEvent::TapeTrade(trade) => {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BaseStrategy][{}] TRADE | price=${:.2} | size={} | timestamp_ms={}",
                        ev.symbol,
                        trade.price,
                        trade.size,
                        trade.timestamp_ms,
                    ),
                );
            }


            // ========================================================
            // QUOTE
            // ========================================================

            AppEvent::Quote(quote) => {

                // Live entry flow:
                //
                // Quote event
                // → restore DateTime<Utc> from timestamp_ms
                // → regular-hours filter
                // → reset state for a new market session
                // → calculate strategy trading window
                // → optionally track prices during opening delay
                // → optionally stop tracking while already in a trade
                // → update rolling price window
                // → check entry gates
                // → inspect EngineView
                // → construct canonical TradeIntent
                // → EngineEvent::OpenTrade

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[BaseStrategy][{}] QUOTE | bid=${:.4} ({}) | ask=${:.4} ({}) | timestamp_ms={}",
                        ev.symbol,
                        quote.bid_price,
                        quote.bid_size,
                        quote.ask_price,
                        quote.ask_size,
                        quote.timestamp_ms,
                    ),
                );


                // ====================================================
                // LIVE EVENT TIME
                // ====================================================

                // Use the exchange/event timestamp carried by the quote rather
                // than wall-clock time so all strategy decisions are tied to
                // the actual market event that triggered them.
                let Some(timestamp): Option<DateTime<Utc>> =
                    DateTime::from_timestamp_millis(quote.timestamp_ms)
                else {
                    eprintln!(
                        "[BASE LIVE][{}] invalid timestamp_ms={}",
                        ev.symbol,
                        quote.timestamp_ms,
                    );

                    return;
                };


                // Use the midpoint between the current best bid and ask
                // as the strategy's current reference market price.
                let mid =
                    (quote.bid_price + quote.ask_price) / 2.0;


                // ====================================================
                // REGULAR MARKET SESSION
                // ====================================================

                let Some((session_start, session_end)) =
                    regular_market_session(timestamp)
                else {
                    return;
                };

                // Ignore all premarket and after-hours quotes.
                if timestamp < session_start || timestamp >= session_end {
                    return;
                }


                // ====================================================
                // NEW SESSION RESET
                // ====================================================

                let previous_session =
                    self.current_session_start_by_symbol
                        .get(&ev.symbol)
                        .copied();

                if previous_session != Some(session_start) {
                    self.window_prices.remove(&ev.symbol);

                    self.current_session_start_by_symbol.insert(
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


                // ====================================================
                // STRATEGY TRADING WINDOW
                // ====================================================

                let Some((trading_start, trading_end)) =
                    strategy_trading_window(
                        timestamp,
                        self.market_open_delay_minutes,
                        self.market_close_buffer_minutes,
                    )
                else {
                    return;
                };


                // ====================================================
                // END-OF-SESSION FORCE CLOSE
                // ====================================================

                // market_close_buffer_minutes:
                // Stops NEW entries before market close.
                //
                // close_all_before_market_close_minutes:
                // Requests closure of remaining trades.
                let close_all_time =
                    session_end
                        - chrono::Duration::minutes(
                            self.close_all_before_market_close_minutes
                        );

                if timestamp >= close_all_time {
                    if self.close_all_sent_for_session != Some(session_start) {
                        self.close_all_sent_for_session = Some(session_start);

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

                    return;
                }


                // ====================================================
                // OPENING DELAY PRICE TRACKING
                // ====================================================

                if !self.track_prices_during_open_delay
                    && timestamp < trading_start
                {
                    return;
                }


                // ====================================================
                // ENGINE VIEW
                // ====================================================

                let view: Arc<EngineView> =
                    self.rx_view.borrow().clone();


                // ====================================================
                // OPEN TRADE PRICE-WINDOW TRACKING
                // ====================================================

                // This remains a strategy-level duplicate-entry gate.
                //
                // Authoritative engine risk checks must still consider
                // trades, working orders, and reserved exposure separately.
                if !self.track_price_window_while_in_trade
                    && view.trades.has_active_trade_for_symbol(&ev.symbol)
                {
                    if self.window_prices.remove(&ev.symbol).is_some() {
                        print_log(
                            self.debug_mode,
                            false,
                            format!(
                                "[BASE LIVE WINDOW CLEARED][{}] \
                                active trade detected; price window cleared",
                                ev.symbol,
                            ),
                        );
                    }

                    return;
                }


                // ====================================================
                // ROLLING PRICE WINDOW
                // ====================================================

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


                    // ====================================================
                    // ENTRY TIME GATE
                    // ====================================================

                    if timestamp < trading_start {
                        return;
                    }

                    if timestamp >= trading_end {
                        return;
                    }


                    // ====================================================
                    // EXISTING ACTIVE TRADE GATE
                    // ====================================================

                    if view.trades.has_active_trade_for_symbol(&ev.symbol) {
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


                    // ====================================================
                    // SHORT ENTRY
                    // ====================================================

                    // Price has risen by the configured percentage from
                    // the rolling low. Enter short expecting mean reversion.
                    if pct_from_min >= self.entry_percent {
                        let intent = TradeIntent {
                            strategy_id: self.name().to_string(),
                            symbol: ev.symbol.clone(),
                            direction: Direction::Short,
                            signal_px: mid,
                            target_notional: self.trade_amount_usd,
                            order_type: OrderType::Market,
                            time_in_force: TimeInForce::Day,
                            limit_px: None,
                            stop_px: None,
                            post_only: false,
                            take_profit_percent: Some(self.exit_profit_percent),
                            stop_loss_percent: Some(self.exit_loss_percent),
                            emergency_stop_percent: Some(self.emergency_exit_percent),
                            stop_loss_delay_secs: Some(self.min_sl_delay),
                            max_duration_secs: if self.enable_trade_duration {
                                self.trade_duration_secs
                            } else {
                                None
                            },
                            ts_ms: quote.timestamp_ms,
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

                        let _ =
                            self.tx_engine.send(
                                EngineEvent::OpenTrade(
                                    intent
                                )
                            );
                    }


                    // ====================================================
                    // LONG ENTRY
                    // ====================================================

                    // Price has fallen by the configured percentage from
                    // the rolling high. Enter long expecting mean reversion.
                    else if pct_from_max >= self.entry_percent {
                        let intent = TradeIntent {
                            strategy_id: self.name().to_string(),
                            symbol: ev.symbol.clone(),
                            direction: Direction::Long,
                            signal_px: mid,
                            target_notional: self.trade_amount_usd,
                            order_type: OrderType::Market,
                            time_in_force: TimeInForce::Day,
                            limit_px: None,
                            stop_px: None,
                            post_only: false,
                            take_profit_percent: Some(self.exit_profit_percent),
                            stop_loss_percent: Some(self.exit_loss_percent),
                            emergency_stop_percent: Some(self.emergency_exit_percent),
                            stop_loss_delay_secs: Some(self.min_sl_delay),
                            max_duration_secs: if self.enable_trade_duration {
                                self.trade_duration_secs
                            } else {
                                None
                            },
                            ts_ms: quote.timestamp_ms,
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

                        let _ =
                            self.tx_engine.send(
                                EngineEvent::OpenTrade(
                                    intent
                                )
                            );
                    }
                }


                // ====================================================
                // QUOTE LOG
                // ====================================================

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


            // ========================================================
            // BAR CLOSED
            // ========================================================

            AppEvent::BarClosed(bar) => {
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

                self.candle_history.push(bar);
            }
        }
    }
}