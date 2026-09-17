#![allow(dead_code)]

// ============================================================================
// ADVANCED LIMIT ORDER TEST STRATEGY
// ============================================================================
//
// Live strategy used to test multiple simultaneous limit orders per asset.
//
// For each configured asset, the strategy submits:
//
//     - Two buy limit orders below the current market price.
//     - Two sell limit orders above the current market price.
//     - One marketable buy limit order above the current market price.
//
// The four orders away from market are expected to rest while the marketable
// limit order is expected to execute immediately.
//
// After the configured resting period:
//
//     - Any remaining resting orders are canceled.
//     - Any successfully opened trade is closed normally.
//     - The strategy waits for the cycle to finish before repeating.
//
// Example:
//
//                     SELL +5%   -> resting
//                     SELL +3%   -> resting
//
//                 CURRENT MARKET PRICE
//
//                     BUY  -3%   -> resting
//                     BUY  -5%   -> resting
//
//                     BUY  +1%   -> expected immediate execution
//
// Each asset runs independently, allowing many simultaneous limit orders
// across multiple symbols.
//
// This is a test strategy only and is not intended to represent a profitable
// or production market-making strategy.
// ============================================================================

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{
    mpsc::UnboundedSender as TokioUnboundedSender,
    watch,
};

use crate::{
    core::{
        engine::engine::EngineEvent,
        engine_view::EngineView,
        events::{AppEvent, AppEventEnvelope},
        execution::{
            ExitOrderRequest,
            order::{OrderId, OrderPurpose, OrderState, OrderType, TimeInForce},
        },
        trade_intent::TradeIntent,
        trades::{Direction, TradeId, TradeState},
    },
    shared::print_log,
    strategies::Strategy,
};


// ============================================================================
// ASSET STATE
// ============================================================================

#[derive(Debug, Clone)]
enum LimitOrderAdvancedAssetPhase {
    WaitingToSubmit,
    DiscoveringOrders { submitted_at: Instant },
    Running { started_at: Instant },
    WaitingForCleanup,
    WaitingToRepeat { repeat_at: Instant },
    Finished,
}

#[derive(Debug, Clone)]
struct LimitOrderAdvancedAssetState {
    phase: LimitOrderAdvancedAssetPhase,
    completed_cycles: usize,

    /// Orders belonging to the current test cycle.
    order_ids: HashSet<OrderId>,

    /// Trades belonging to the current test cycle.
    trade_ids: HashSet<TradeId>,

    /// Orders already sent a cancellation request.
    cancel_requested: HashSet<OrderId>,

    /// Trades already sent a close request.
    close_requested: HashSet<TradeId>,
}


// ============================================================================
// STRATEGY
// ============================================================================

pub struct LimitOrderAdvancedStrategy {
    pub enable_strategy: bool,
    pub debug_mode: bool,

    tx_engine: TokioUnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,

    // ========================================================================
    // TEST CONFIGURATION
    // ========================================================================

    allowed_assets: Vec<String>,

    /// USD notional for each individual order.
    trade_amount_usd: f64,

    /// Resting buy offsets below market.
    resting_buy_offsets: Vec<f64>,

    /// Resting sell offsets above market.
    resting_sell_offsets: Vec<f64>,

    /// Marketable buy limit offset above market.
    marketable_buy_offset: f64,

    /// Delay before the first cycle begins.
    first_trade_delay: Duration,

    /// Amount of time resting orders remain live before cancellation.
    resting_duration: Duration,

    /// Minimum amount of time an executed trade remains open before closing.
    close_delay: Duration,

    /// Delay before beginning another cycle.
    repeat_delay: Duration,

    /// Number of complete test cycles per asset.
    max_cycles_per_asset: usize,

    started_at: Instant,
    asset_state: HashMap<String, LimitOrderAdvancedAssetState>,
}


// ============================================================================
// CONSTRUCTOR
// ============================================================================

impl LimitOrderAdvancedStrategy {
    pub fn new(
        tx_engine: TokioUnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let allowed_assets = vec![
            "BTC".to_string(),
            "ETH".to_string(),
            // "SOL".to_string(),
        ];

        let asset_state = allowed_assets
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    LimitOrderAdvancedAssetState {
                        phase: LimitOrderAdvancedAssetPhase::WaitingToSubmit,
                        completed_cycles: 0,
                        order_ids: HashSet::new(),
                        trade_ids: HashSet::new(),
                        cancel_requested: HashSet::new(),
                        close_requested: HashSet::new(),
                    },
                )
            })
            .collect();

        Self {
            enable_strategy: true,
            debug_mode: true,
            tx_engine,
            rx_view,
            allowed_assets,

            trade_amount_usd: 13.0,

            // 3% and 5% below market.
            resting_buy_offsets: vec![0.03, 0.05],

            // 3% and 5% above market.
            resting_sell_offsets: vec![0.03, 0.05],

            // A buy limit above market should be immediately marketable.
            marketable_buy_offset: 0.01,

            first_trade_delay: Duration::from_secs(10),
            resting_duration: Duration::from_secs(10),
            close_delay: Duration::from_secs(10),
            repeat_delay: Duration::from_secs(10),

            max_cycles_per_asset: 1,

            started_at: Instant::now(),
            asset_state,
        }
    }


    // ========================================================================
    // SYMBOL ALLOWLIST
    // ========================================================================

    fn is_allowed_asset(&self, symbol: &str) -> bool {
        self.allowed_assets.iter().any(|allowed| allowed == symbol)
    }


    // ========================================================================
    // SEND LIMIT ORDER
    // ========================================================================

    fn send_limit(
        &self,
        symbol: &str,
        direction: Direction,
        signal_px: f64,
        limit_px: f64,
        ts_ms: i64,
        label: &str,
    ) -> bool {
        let intent = TradeIntent {
            strategy_id: self.name().to_string(),
            symbol: symbol.to_string(),
            direction,
            signal_px,
            target_notional: self.trade_amount_usd,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            limit_px: Some(limit_px),
            stop_px: None,
            post_only: false,

            take_profit_percent: None,
            stop_loss_percent: None,
            emergency_stop_percent: None,
            stop_loss_delay_secs: None,
            max_duration_secs: None,

            ts_ms,
        };

        print_log(
            self.debug_mode,
            true,
            format!(
                "[LimitOrderAdvancedStrategy][{}] SUBMIT {} | direction={:?} | notional=${:.2} | market_px={} | limit_px={}",
                symbol,
                label,
                direction,
                self.trade_amount_usd,
                signal_px,
                limit_px,
            ),
        );

        self.tx_engine.send(EngineEvent::OpenTrade(intent)).is_ok()
    }


    // ========================================================================
    // SUBMIT TEST ORDER SET
    // ========================================================================

    fn submit_order_set(&self, symbol: &str, market_px: f64, ts_ms: i64) {
        let buy_1_px = market_px * (1.0 - self.resting_buy_offsets[0]);
        let buy_2_px = market_px * (1.0 - self.resting_buy_offsets[1]);

        let sell_1_px = market_px * (1.0 + self.resting_sell_offsets[0]);
        let sell_2_px = market_px * (1.0 + self.resting_sell_offsets[1]);

        let marketable_buy_px = market_px * (1.0 + self.marketable_buy_offset);

        self.send_limit(symbol, Direction::Long, market_px, buy_1_px, ts_ms, "RESTING BUY #1");
        self.send_limit(symbol, Direction::Long, market_px, buy_2_px, ts_ms, "RESTING BUY #2");

        self.send_limit(symbol, Direction::Short, market_px, sell_1_px, ts_ms, "RESTING SELL #1");
        self.send_limit(symbol, Direction::Short, market_px, sell_2_px, ts_ms, "RESTING SELL #2");

        self.send_limit(symbol, Direction::Long, market_px, marketable_buy_px, ts_ms, "MARKETABLE BUY");
    }
}


// ============================================================================
// STRATEGY IMPLEMENTATION
// ============================================================================

#[async_trait::async_trait]
impl Strategy for LimitOrderAdvancedStrategy {
    fn name(&self) -> &'static str {
        "LimitOrderAdvancedStrategy"
    }

    async fn on_event(&mut self, ev: AppEventEnvelope) {
        if !self.enable_strategy {
            return;
        }

        if !self.is_allowed_asset(&ev.symbol) {
            return;
        }


        // ====================================================================
        // MARKET PRICE / TIMESTAMP
        // ====================================================================

        let (market_px, ts_ms) = match ev.event {
            AppEvent::TapeTrade(trade) => (trade.price, trade.timestamp_ms),
            AppEvent::Quote(quote) => ((quote.bid_price + quote.ask_price) / 2.0, quote.timestamp_ms),
            AppEvent::BarClosed(_) => return,
        };


        // ====================================================================
        // ENGINE VIEW / CURRENT STATE
        // ====================================================================

        let view = self.rx_view.borrow().clone();

        let Some(current_state) = self.asset_state.get(&ev.symbol).cloned() else {
            return;
        };


        // ====================================================================
        // STATE MACHINE
        // ====================================================================

        match current_state.phase {

            // =================================================================
            // WAITING TO SUBMIT
            // =================================================================

            LimitOrderAdvancedAssetPhase::WaitingToSubmit => {
                if current_state.completed_cycles >= self.max_cycles_per_asset {
                    if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                        state.phase = LimitOrderAdvancedAssetPhase::Finished;
                    }

                    return;
                }

                if current_state.completed_cycles == 0 && self.started_at.elapsed() < self.first_trade_delay {
                    return;
                }

                // Snapshot existing order IDs so newly created orders can be
                // discovered after the five intents are submitted.
                let existing_order_ids: HashSet<OrderId> = view.orders
                    .iter()
                    .filter(|(_, order)| order.strategy_id == self.name() && order.symbol == ev.symbol)
                    .map(|(order_id, _)| *order_id)
                    .collect();

                self.submit_order_set(&ev.symbol, market_px, ts_ms);

                if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                    state.order_ids = existing_order_ids;
                    state.trade_ids.clear();
                    state.cancel_requested.clear();
                    state.close_requested.clear();
                    state.phase = LimitOrderAdvancedAssetPhase::DiscoveringOrders { submitted_at: Instant::now() };
                }
            }


            // =================================================================
            // DISCOVER THE FIVE NEW ORDERS
            // =================================================================

            LimitOrderAdvancedAssetPhase::DiscoveringOrders { submitted_at } => {
                let current_orders: Vec<(OrderId, Option<TradeId>)> = view.orders
                    .iter()
                    .filter(|(_, order)| order.strategy_id == self.name() && order.symbol == ev.symbol)
                    .filter(|(order_id, _)| !current_state.order_ids.contains(order_id))
                    .map(|(order_id, order)| (*order_id, order.trade_id))
                    .collect();

                if current_orders.len() < 5 {
                    if submitted_at.elapsed() > Duration::from_secs(10) {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[LimitOrderAdvancedStrategy][{}] WAITING FOR ORDERS | discovered={}/5",
                                ev.symbol,
                                current_orders.len(),
                            ),
                        );
                    }

                    return;
                }

                if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                    state.order_ids = current_orders.iter().map(|(order_id, _)| *order_id).collect();
                    state.trade_ids = current_orders.iter().filter_map(|(_, trade_id)| *trade_id).collect();
                    state.phase = LimitOrderAdvancedAssetPhase::Running { started_at: Instant::now() };
                }

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderAdvancedStrategy][{}] ORDER SET CREATED | orders={} | trades={}",
                        ev.symbol,
                        current_orders.len(),
                        current_orders.iter().filter_map(|(_, trade_id)| *trade_id).count(),
                    ),
                );
            }


            // =================================================================
            // RUNNING
            // =================================================================

            LimitOrderAdvancedAssetPhase::Running { started_at } => {

                // ----------------------------------------------------------------
                // CLOSE ANY TRADE THAT SUCCESSFULLY OPENED
                // ----------------------------------------------------------------

                if started_at.elapsed() >= self.close_delay {
                    let mut trades_to_close = Vec::new();

                    for trade_id in &current_state.trade_ids {
                        if current_state.close_requested.contains(trade_id) {
                            continue;
                        }

                        let Some(trade) = view.trades.get(*trade_id) else {
                            continue;
                        };

                        if trade.state == TradeState::Open {
                            trades_to_close.push(*trade_id);
                        }
                    }

                    for trade_id in trades_to_close {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[LimitOrderAdvancedStrategy][{}] CLOSE FILLED TRADE | trade_id={}",
                                ev.symbol,
                                trade_id,
                            ),
                        );

                        if self.tx_engine.send(
                            EngineEvent::CloseTrade {
                                trade_id,
                                execution: ExitOrderRequest::market(),
                                reason: "LimitOrderAdvancedStrategy filled limit-order test".to_string(),
                                ts_ms,
                            }
                        ).is_ok() {
                            if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                                state.close_requested.insert(trade_id);
                            }
                        }
                    }
                }


                // ----------------------------------------------------------------
                // CANCEL REMAINING RESTING ENTRY ORDERS
                // ----------------------------------------------------------------

                if started_at.elapsed() >= self.resting_duration {
                    let mut orders_to_cancel = Vec::new();

                    for order_id in &current_state.order_ids {
                        if current_state.cancel_requested.contains(order_id) {
                            continue;
                        }

                        let Some(order) = view.orders.get(*order_id) else {
                            continue;
                        };

                        if order.purpose != OrderPurpose::Entry {
                            continue;
                        }

                        if matches!(
                            order.state,
                            OrderState::Working |
                            OrderState::PendingSubmit
                        ) {
                            orders_to_cancel.push(*order_id);
                        }
                    }

                    for order_id in orders_to_cancel {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[LimitOrderAdvancedStrategy][{}] CANCEL RESTING ORDER | order_id={}",
                                ev.symbol,
                                order_id,
                            ),
                        );

                        if self.tx_engine.send(
                            EngineEvent::CancelOrder {
                                order_id,
                                reason: "LimitOrderAdvancedStrategy resting-order cleanup".to_string(),
                                ts_ms,
                            }
                        ).is_ok() {
                            if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                                state.cancel_requested.insert(order_id);
                            }
                        }
                    }

                    if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                        state.phase = LimitOrderAdvancedAssetPhase::WaitingForCleanup;
                    }
                }
            }


            // =================================================================
            // WAIT FOR ALL ORDERS / TRADES TO FINISH
            // =================================================================

            LimitOrderAdvancedAssetPhase::WaitingForCleanup => {
                let orders_finished = current_state.order_ids.iter().all(|order_id| {
                    view.orders
                        .get(*order_id)
                        .map(|order| order.state.is_terminal())
                        .unwrap_or(true)
                });

                let trades_finished = current_state.trade_ids.iter().all(|trade_id| {
                    view.trades
                        .get(*trade_id)
                        .map(|trade| trade.state == TradeState::Closed)
                        .unwrap_or(true)
                });

                if !orders_finished || !trades_finished {
                    return;
                }

                let completed_cycles = current_state.completed_cycles + 1;

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderAdvancedStrategy][{}] CYCLE COMPLETE | completed_cycles={} | max_cycles={}",
                        ev.symbol,
                        completed_cycles,
                        self.max_cycles_per_asset,
                    ),
                );

                if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                    state.completed_cycles = completed_cycles;
                    state.order_ids.clear();
                    state.trade_ids.clear();
                    state.cancel_requested.clear();
                    state.close_requested.clear();

                    if completed_cycles >= self.max_cycles_per_asset {
                        state.phase = LimitOrderAdvancedAssetPhase::Finished;

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[LimitOrderAdvancedStrategy][{}] TEST FINISHED | completed_cycles={}",
                                ev.symbol,
                                completed_cycles,
                            ),
                        );
                    } else {
                        state.phase = LimitOrderAdvancedAssetPhase::WaitingToRepeat {
                            repeat_at: Instant::now() + self.repeat_delay,
                        };
                    }
                }
            }


            // =================================================================
            // WAIT BEFORE NEXT CYCLE
            // =================================================================

            LimitOrderAdvancedAssetPhase::WaitingToRepeat { repeat_at } => {
                if Instant::now() < repeat_at {
                    return;
                }

                if let Some(state) = self.asset_state.get_mut(&ev.symbol) {
                    state.phase = LimitOrderAdvancedAssetPhase::WaitingToSubmit;
                }

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderAdvancedStrategy][{}] REPEAT DELAY COMPLETE | next_cycle={}",
                        ev.symbol,
                        current_state.completed_cycles + 1,
                    ),
                );
            }


            // =================================================================
            // FINISHED
            // =================================================================

            LimitOrderAdvancedAssetPhase::Finished => return,
        }
    }
}