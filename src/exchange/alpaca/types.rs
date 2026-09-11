#![allow(dead_code)]

use serde::Deserialize;

// ============================================================
// ALPACA CONFIG
// ============================================================

#[derive(Debug, Clone)]
pub struct AlpacaConfig {
    /// Alpaca API key used for REST and trade-update WebSocket authentication.
    pub api_key: String,

    /// Alpaca API secret used for REST and trade-update WebSocket authentication.
    pub api_secret: String,

    /// Alpaca Trading API REST base URL.
    pub base_url: String,

    /// True when using Alpaca paper trading and false when using live trading.
    pub paper_trading: bool,
}


// ============================================================
// ALPACA ACCOUNT BALANCE
// ============================================================

#[derive(Debug, Default, Clone, Deserialize)]
pub struct AlpacaAccountBalance {
    /// Current Alpaca account status.
    ///
    /// A normal active trading account should generally report `ACTIVE`.
    pub status: String,

    /// Cash currently held in the Alpaca account.
    pub cash: String,

    /// Current buying power available for opening new positions.
    pub buying_power: String,

    /// Current account equity including cash and open positions.
    pub equity: String,

    /// Current total portfolio value reported by Alpaca.
    pub portfolio_value: String,

    /// Current market value of all long positions.
    pub long_market_value: String,

    /// Current market value of all short positions.
    pub short_market_value: String,
}


// ============================================================
// ALPACA ACCOUNT BALANCE HELPERS
// ============================================================

impl AlpacaAccountBalance {

    /// Creates an account balance with a specified starting cash amount.
    ///
    /// This is primarily used by backtesting where there is no real Alpaca
    /// account and the engine needs a configurable starting balance.
    ///
    /// Cash, buying power, equity, and portfolio value begin at the supplied
    /// amount while long and short market values begin at zero.
    pub fn with_cash(
        cash: f64,
    ) -> Self {
        let cash =
            cash.to_string();


        Self {
            status:
                "ACTIVE".to_string(),

            cash:
                cash.clone(),

            buying_power:
                cash.clone(),

            equity:
                cash.clone(),

            portfolio_value:
                cash,

            long_market_value:
                "0".to_string(),

            short_market_value:
                "0".to_string(),
        }
    }


    /// Updates only the account's available cash value.
    ///
    /// Other account values such as buying power, equity, portfolio value,
    /// and current market exposure are intentionally left unchanged.
    pub fn set_cash(
        &mut self,
        cash: f64,
    ) {
        self.cash =
            cash.to_string();
    }
}


// ============================================================
// TRADE UPDATES CONNECTION STATE
// ============================================================

/// Current health state of Alpaca's execution `trade_updates` WebSocket.
///
/// The execution adapter uses this state to avoid submitting new live orders
/// while fill/order monitoring is unavailable.
///
/// This is intentionally separate from the market-data WebSocket state.
/// Market prices and execution updates use independent Alpaca connections and
/// can therefore disconnect or recover independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TradeUpdatesConnectionState {
    Connecting,
    Connected,
    Disconnected,
}


// ============================================================
// ORDER CORRELATION
// ============================================================

/// Identifies the engine-side purpose of an Alpaca venue order.
///
/// This is temporary adapter correlation state used to translate Alpaca
/// `trade_updates` back into the appropriate canonical execution flow.
///
/// TODO:
/// Revisit this enum once the new canonical Order / OrderRole model is fully
/// wired. Ideally the engine order itself will carry enough information that
/// the adapter does not need a separate simplified Open / Close classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderKind {
    Open,
    Close,
}


/// Minimal Alpaca adapter state used to correlate a venue order ID with the
/// canonical engine request that originally caused the order submission.
///
/// This state belongs to the Alpaca adapter rather than the engine. The engine
/// remains authoritative for canonical order, trade, fill, position, and risk
/// state.
///
/// TODO:
/// Replace the temporary String-based `cid` with the canonical engine `OrderId`
/// or execution correlation identifier once the final engine execution model
/// has been wired end-to-end.
#[derive(Debug, Clone)]
pub(crate) struct OrderCorrelation {
    /// Engine-generated client/correlation identifier.
    pub cid: String,

    /// Symbol associated with the order.
    pub symbol: String,

    /// Whether this venue order belongs to an opening or closing flow.
    pub kind: OrderKind,
}