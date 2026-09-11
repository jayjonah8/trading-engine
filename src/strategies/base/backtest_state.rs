use std::{
    collections::{
        HashMap,
        VecDeque,
    },
    sync::Arc,
};

use chrono::{
    DateTime,
    Utc,
};

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::core::{
    engine::engine::EngineEvent,
    engine_view::EngineView,
};

use crate::market::candle_history::CandleHistory;


pub struct BaseBacktestStrategy {
    // Sends strategy-generated events and trade actions to the trading engine.
    pub tx_engine: UnboundedSender<EngineEvent>,

    // Receives the latest shared engine state and configuration updates.
    pub rx_view: watch::Receiver<Arc<EngineView>>,

    // Master On/Off for this backtest_strategy.rs.
    pub enable_strategy: bool,

    // On/Off for all debug messages.
    pub debug_mode: bool,

    // Total historical market events processed.
    pub total_trade_events: u64,

    // Rolling historical price window for each symbol.
    //
    // Unlike the live strategy, this uses DateTime<Utc> so the window
    // advances using historical market time rather than real-world time.
    //
    // Upward movement example:
    // If AAPL's lowest price during the last 30 minutes was $100.00
    // and the current price reaches $101.00, the price has moved +1.0%.
    //
    // Downward movement example:
    // If AAPL's highest price during the last 30 minutes was $100.00
    // and the current price falls to $99.00, the price has moved -1.0%.
    pub window_prices:
        HashMap<String, VecDeque<(DateTime<Utc>, f64)>>,

    // Length of historical market time retained in the rolling price window.
    pub entry_window_secs: i64,

    // Amount of USD allocated to each trade before leverage (e.g. 100.0 = $100)
    pub trade_amount_usd: f64,

    // Minimum percentage price movement required to trigger a trade entry.
    pub entry_percent: f64,

    // Profit percentage at which the position should be closed (e.g. 3.0 = 3%)
    pub exit_profit_percent: f64,

    // Minimum percentage price movement required to trigger a trade entry.
    pub exit_loss_percent: f64,

    // Emergency stop-loss
    pub emergency_exit_percent: f64,

    // Duration in seconds to delay hard stop-loss
    pub min_sl_delay: u64,

    // Mode to limit how long a trade can be open
    pub enable_trade_duration: bool,

    // Time limit in seconds until an open trade is auto closed
    pub trade_duration_secs: Option<u64>,

    // true  = keep tracking the symbol's rolling price window while a trade is open
    // false = clear that symbol's rolling window and stop tracking it until the trade is closed
    pub track_price_window_while_in_trade: bool,

    // Stores completed candlestick history for each symbol and timeframe.
    pub candle_history: CandleHistory,

    // Number of minutes after market open before new trade entries are allowed.
    pub market_open_delay_minutes: i64,

    // Number of minutes before market close when new trade entries are disabled.
    pub market_close_buffer_minutes: i64,

    // Force-close all remaining trades this many minutes before market close.
    pub close_all_before_market_close_minutes: i64,

    /// Tracks whether the end-of-session close-all request has already
    /// been sent for the current market session, preventing duplicate requests.
    pub close_all_sent_for_session: Option<DateTime<Utc>>,

    // Controls whether prices collected during the market-open delay
    // are included in the rolling price window.
    pub track_prices_during_open_delay: bool,

    // Tracks the current market session for each symbol so the rolling
    // price window can be reset when a new trading day begins.
    pub current_session_start_by_symbol:
        HashMap<String, DateTime<Utc>>,
}