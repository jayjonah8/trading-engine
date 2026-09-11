use std::sync::Arc;

use std::{
    collections::{HashMap, VecDeque},
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

use crate::core::{
    engine::engine::EngineEvent,
    engine_view::EngineView,
};
use crate::market::{
    candle_history::CandleHistory,
};

#[allow(dead_code)]
pub struct BaseStrategy {
    // Stragey on/off switch
    pub enable_strategy: bool,

    // Enable additional strategy debug logging
    pub debug_mode: bool,

    // Strategy -> Engine messages
    pub tx_engine: TokioUnboundedSender<EngineEvent>,

    // Read-only Engine state
    pub rx_view: watch::Receiver<Arc<EngineView>>,

    // Per-symbol rolling price history: symbol -> [(timestamp, price), ...]
    pub window_prices: HashMap<String, VecDeque<(Instant, f64)>>,

    // Rolling price window duration in seconds (e.g. 1500 = 25 minutes)
    pub entry_window_secs: u64,

    // Amount of USD allocated to each trade before leverage (e.g. 100.0 = $100)
    pub trade_amount_usd: f64,

    // Percentage move from a window extreme required to trigger an entry (e.g. 2.0 = 2%)
    pub entry_percent: f64,

    // Profit percentage at which the position should be closed (e.g. 3.0 = 3%)
    pub exit_profit_percent: f64,

    // Loss percentage at which the normal stop-loss should close the position (e.g. 2.0 = 2%)
    pub exit_loss_percent: f64,

    // Loss percentage at which the emergency stop-loss immediately closes the position (e.g. 5.0 = 5%)
    pub emergency_exit_percent: f64,

    // Minimum seconds before the normal stop-loss is allowed to trigger (e.g. 30 = 30 seconds)
    pub min_sl_delay: u64,

    // Whether positions should automatically close after reaching the maximum trade duration
    pub enable_trade_duration: bool,

    // Maximum time a position may remain open in seconds (e.g. Some(3600) = 1 hour, None = unlimited)
    pub trade_duration_secs: Option<u64>,

    // true  = keep tracking the symbol's rolling price window while a trade is open
    // false = clear that symbol's rolling window and stop tracking it until the trade is closed
    pub track_price_window_while_in_trade: bool,

    // Stores closed candles and automatically retains
    // only the configured amount of candle history.
    pub candle_history: CandleHistory,

    /// Number of minutes after the regular market open before
    /// the strategy is allowed to begin opening new trades.
    pub market_open_delay_minutes: i64,

    /// Number of minutes before the regular market close when
    /// the strategy must stop opening new trades.
    pub market_close_buffer_minutes: i64,

    /// Number of minutes before the regular market close when
    /// all remaining open trades should be force-closed.
    pub close_all_before_market_close_minutes: i64,

    /// Tracks the market session for which the close-all request
    /// has already been sent, preventing duplicate requests.
    pub close_all_sent_for_session: Option<DateTime<Utc>>,

    /// Controls whether prices collected during the market-open
    /// delay are included in the rolling strategy price window.
    pub track_prices_during_open_delay: bool,

    /// Tracks the current regular market session for each symbol
    /// so symbol-specific rolling state can reset each trading day.
    pub current_session_start_by_symbol: HashMap<String, DateTime<Utc>>,
}