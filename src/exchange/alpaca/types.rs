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
// ALPACA ASSET INFO
// ============================================================
#[derive(Debug, Clone, Deserialize)]
pub struct AlpacaAsset {
    pub id: String,
    pub class: String,
    pub exchange: String,
    pub symbol: String,
    pub name: String,
    pub status: String,
    pub tradable: bool,
    pub marginable: bool,
    pub maintenance_margin_requirement: f64,
    pub shortable: bool,
    pub easy_to_borrow: bool,
    pub fractionable: bool,
}