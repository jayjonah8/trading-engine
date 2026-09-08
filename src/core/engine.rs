#![allow(dead_code)]

use std::sync::Arc;
use std::collections::{HashMap};

use tokio::sync::{
    mpsc::UnboundedSender,
    oneshot,
    watch,
};

use crate::core::engine_state::{
    Engine,
    TradingPhase,
    LastPrice,
};
use super::config::Config;
use super::engine_view::EngineView;
use super::general::General;

use crate::core::trade_intent::TradeIntent;
use crate::core::exec::{ExecCommand, ExecEvent};
use crate::core::trades::{
    PendingCloses,
    PendingOpens,
    Trades,
};

use crate::notifications::env_helpers::validate_notification_env;

use crate::core::engine_shared::{
    get_execution_mode,
    verify_engine_config,
};

use crate::exchange::alpaca_utils::{
    AlpacaAccountBalance,
    get_alpaca_account_balance,
};

pub enum EngineEvent {
    /// Requests a new live or simulated trade.
    OpenMarketOrder(TradeIntent),

    /// Requests a new trade during historical backtesting.
    OpenMarketOrderBacktest(TradeIntent),

    /// Requests a new live or simulated limit order.
    OpenLimitOrder(TradeIntent),

    /// Requests a new limit order during historical backtesting.
    OpenLimitOrderBacktest(TradeIntent),

    /// Requests cancellation of an active live or simulated limit (or resting) order. 
    CancelOrder { order_id: String, reason: String },

    /// Requests cancellation of an active limit order during historical backtesting.
    CancelOrderBacktest { order_id: String, reason: String },

    /// Requests modification of an active live or simulated order.
    ///
    /// Quantity or limit price may be omitted when that value should remain unchanged.
    ModifyLiveOrder { order_id: String, quantity: Option<f64>, limit_px: Option<f64> },

    /// Requests modification of an active order during historical backtesting.
    ModifyLiveOrderBacktest { order_id: String, quantity: Option<f64>, limit_px: Option<f64> },

    /// Requests that one specific live or simulated trade be closed by symbol.
    CloseSingleTrade { symbol: String, reason: String },

    /// Requests that one specific trade be closed by symbol during historical backtesting.
    CloseSingleTradeBacktest { symbol: String, reason: String },

    /// Requests that all currently open live or simulated trades be closed.
    ///
    /// Useful for end-of-session cleanup, emergency shutdown,
    /// or other strategy-wide exit conditions.
    CloseAllTrades { reason: String },

    /// Requests that all currently open trades be closed during historical backtesting.
    CloseAllTradesBacktest { reason: String },

    /// Live market-price update used for position valuation
    /// and live/simulation risk management.
    PriceTick { symbol: String, px: f64, ts_ms: i64 },

    /// Historical market-price update used during backtesting.
    ///
    /// Uses the original historical timestamp so risk checks and
    /// trade behavior remain tied to replay time instead of wall time.
    PriceTickBacktest { symbol: String, px: f64, ts_ms: i64 },

    /// Finalizes a completed live or simulated trade.
    ///
    /// Live trades reach this event after the exchange confirms the final
    /// CloseFill. Simulated trades reach it directly after the local risk
    /// checker determines the simulated exit.
    ///
    /// This provides one shared path for final close bookkeeping such as
    /// realized PnL, trade counters, symbol ownership, and trade removal.
    TradeClosed { id: String, ts_ms: i64, realized: f64, exit_px: f64 },

    /// Finalizes a completed historical backtest trade.
    ///
    /// Backtest trades reach this event after a historical risk condition,
    /// strategy exit, or forced close determines the simulated exit.
    ///
    /// Uses historical replay timestamps rather than wall-clock time.
    TradeClosedBacktest { id: String, ts_ms: i64, realized: f64, exit_px: f64 },

    /// Backtest synchronization barrier.
    ///
    /// Confirms that all EngineEvents queued before this event
    /// have been processed before the backtest continues.
    BacktestBarrier { done: oneshot::Sender<()> },
}


impl Engine {

    pub async fn new(
        cfg: Config,
        tx_view: watch::Sender<Arc<EngineView>>,
        tx_engine: UnboundedSender<EngineEvent>,
        tx_exec: UnboundedSender<ExecCommand>,
    ) -> Self {

        verify_engine_config(&cfg);
        validate_notification_env(&cfg);

        let execution_mode = get_execution_mode(&cfg);

        // ============================================================
        // INITIALIZE ACCOUNT BALANCE
        // ============================================================

        let account_balance =
            if cfg.enable_backtest_mode {
                AlpacaAccountBalance::with_cash(
                    cfg.backtest_starting_balance
                )
            } else {
                get_alpaca_account_balance()
                    .await
                    .expect(
                        "Failed to fetch Alpaca account balance"
                    )
            };

        // ============================================================
        // INITIALIZE GENERAL STATE
        // ============================================================

        let general =
            General {
                realized_pnl: 0.0,
                sim_realized_pnl: 0.0,
                account_balance,
            };

        // ============================================================
        // INITIALIZE ENGINE
        // ============================================================

        let me =
            Self {
                cfg,
                general,
                trades: Trades::default(),
                pending_opens: PendingOpens::default(),
                pending_closes: PendingCloses::default(),
                last_px: HashMap::new(),
                tx_view,
                tx_engine,
                tx_exec,
                execution_mode,
                trading_phase: TradingPhase::Normal,
                owner_by_symbol: HashMap::new(),
                last_decision_ts: HashMap::new(),
                open_count_by_symbol: HashMap::new(),
                debug_mode: true,
            };


        me.publish_view();

        me
    }


    // ============================================================
    // ENGINE VIEW (updates engine state for strategies when called)
    // ============================================================   

    fn publish_view(&self) {
        let view = EngineView::from_parts(
            &self.cfg,
            &self.general,
            &self.trades,
        );

        let _ = self.tx_view.send_replace(view);
    }


    // ============================================================
    // ENGINE EVENT DISPATCHER
    // ============================================================

    /// Central entry point for all events processed by the engine.
    ///
    /// Routes strategy intents, market-data updates, execution events,
    /// trade lifecycle events, and backtest events to their dedicated
    /// handlers where authoritative engine state is updated.
    pub fn on_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::OpenMarketOrder(intent) => {
                self.handle_open_trade(intent);
                // @todo error message (intent should block) if MarketOrder OrderType is used in OpenLimitOrder or OpenLimitOrderBacktest message
                // add enum OrderType {MarketOrder, LimitOrder}
            }

            EngineEvent::OpenMarketOrderBacktest(intent) => {
                self.handle_open_backtest_trade(intent);
            }

            EngineEvent::OpenLimitOrder(intent) => {
                // @todo Handle live and simulated limit order creation.
                //  symbol, direction, quantity, limit_px 
                // error message (intent should block) if LimitOrder OrderType is used in OpenMarketOrder or OpenLimitOrderBacktest message
            }

            EngineEvent::OpenLimitOrderBacktest(intent)  => {
                // @todo Handle historical backtest limit order creation.
                // symbol, direction, quantity, limit_px 
                // add enum OrderType {MarketOrder, LimitOrder}
            }

            EngineEvent::CancelOrder { order_id, reason } => {
                // @todo Handle cancellation of an active live or simulated limit order.
                // should cancel limit orders or resting orders
            }

            EngineEvent::CancelOrderBacktest { order_id, reason } => {
                // @todo Handle cancellation of an active backtest limit order.
                // should cancel limit orders or resting orders
            }

            EngineEvent::ModifyLiveOrder { order_id, quantity, limit_px } => {
                // @todo Handle modification of an active live or simulated order.
            }

            EngineEvent::ModifyLiveOrderBacktest { order_id, quantity, limit_px } => {
                // @todo Handle modification of an active backtest order.
            }

            EngineEvent::CloseSingleTrade { symbol, reason } => {
                self.handle_close_single_trade(symbol, reason);
            }

            EngineEvent::CloseSingleTradeBacktest { symbol, reason } => {
                self.handle_close_single_trade_backtest(symbol, reason);
            }

            EngineEvent::CloseAllTrades { reason } => {
                self.handle_close_all_trades(reason);
            }

            EngineEvent::CloseAllTradesBacktest { reason } => {
                self.handle_close_all_trades_backtest(reason);
            }

            EngineEvent::PriceTick { symbol, px, ts_ms } => {
                let sym = symbol.clone();

                self.last_px.insert(symbol, LastPrice { px, ts_ms });
                self.check_risk_for_symbol(&sym, px, ts_ms);
            }

            EngineEvent::PriceTickBacktest { symbol, px, ts_ms } => {
                let sym = symbol.clone();

                self.last_px.insert(symbol, LastPrice { px, ts_ms });
                self.check_risk_for_backtest_symbol(&sym, px, ts_ms);
            }

            EngineEvent::TradeClosed { id, ts_ms, realized, exit_px } => {
                self.handle_trade_closed(id, ts_ms, realized, exit_px);
            }

            EngineEvent::TradeClosedBacktest { id, ts_ms, realized, exit_px } => {
                self.handle_trade_closed_backtest(id, ts_ms, realized, exit_px);
            }

            EngineEvent::BacktestBarrier { done } => {
                let _ = done.send(());
            }
        }

        self.publish_view();
    }


    // ============================================================
    // EXECUTION EVENTS
    // ============================================================

    /// Routes live exchange execution events to the appropriate engine handler.
    ///
    /// These events come from the live exchange adapter and are only used for
    /// real exchange execution. They are not used by Simulation or Backtest modes,
    /// which handle their simulated fills internally without the exchange adapter.
    pub fn on_exec_event(
        &mut self,
        event: ExecEvent,
    ) {
        match event {
            ExecEvent::Ack { .. } => {
                self.handle_exec_ack(event);
            }

            ExecEvent::OpenPartialFill { .. } => {
                self.handle_open_partial_fill(event);
            }

            ExecEvent::OpenFill { .. } => {
                self.handle_open_fill(event);
            }

            ExecEvent::ClosePartialFill { .. } => {
                self.handle_close_partial_fill(event);
            }

            ExecEvent::CloseFill { .. } => {
                self.handle_close_fill(event);
            }

            ExecEvent::Resting { .. } => {
                self.handle_exec_resting(event);
            }

            ExecEvent::Error { .. } => {
                self.handle_exec_error(event);
            }
        }

        self.publish_view();
    }
}