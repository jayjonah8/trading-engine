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
// GET ALPACA CONFIG
// ============================================================

/// Loads the Alpaca trading configuration from environment variables.
///
/// `PAPER_TRADING_MODE` determines whether the adapter connects to the
/// Alpaca paper or live trading environment.
///
/// Paper and live trading use separate API credentials and REST endpoints.
pub fn get_alpaca_config() -> anyhow::Result<AlpacaConfig> {
    let paper_trading =
        std::env::var(
            "PAPER_TRADING_MODE"
        )?
        .parse::<bool>()?;


    if paper_trading {
        Ok(
            AlpacaConfig {
                api_key:
                    std::env::var(
                        "ALPACA_PAPER_API_KEY_ID"
                    )?,

                api_secret:
                    std::env::var(
                        "ALPACA_PAPER_API_SECRET_KEY"
                    )?,

                base_url:
                    "https://paper-api.alpaca.markets"
                        .to_string(),

                paper_trading:
                    true,
            }
        )
    } else {
        Ok(
            AlpacaConfig {
                api_key:
                    std::env::var(
                        "ALPACA_API_KEY_ID"
                    )?,

                api_secret:
                    std::env::var(
                        "ALPACA_API_SECRET_KEY"
                    )?,

                base_url:
                    "https://api.alpaca.markets"
                        .to_string(),

                paper_trading:
                    false,
            }
        )
    }
}


// ============================================================
// GET ALPACA ACCOUNT BALANCE
// ============================================================

/// Fetches the current Alpaca account balance information.
///
/// The active paper or live environment is selected through
/// `get_alpaca_config()`, ensuring this request uses the same credentials
/// and REST endpoint as the production execution adapter.
///
/// Returns the account's cash, buying power, equity, portfolio value,
/// long market value, short market value, and account status.
///
/// Returns an error if the configuration cannot be loaded, the Alpaca
/// request fails, Alpaca returns a non-success HTTP status, or the
/// response cannot be parsed.
pub async fn get_alpaca_account_balance()
    -> anyhow::Result<AlpacaAccountBalance>
{
    let config =
        get_alpaca_config()?;


    // ============================================================
    // REQUEST ACCOUNT
    // ============================================================

    let client =
        reqwest::Client::new();


    let response =
        client
            .get(
                format!(
                    "{}/v2/account",
                    config.base_url
                )
            )
            .header(
                "APCA-API-KEY-ID",
                &config.api_key
            )
            .header(
                "APCA-API-SECRET-KEY",
                &config.api_secret
            )
            .send()
            .await?;


    let status =
        response.status();


    let body =
        response
            .text()
            .await?;


    // ============================================================
    // VALIDATE RESPONSE
    // ============================================================

    if !status.is_success() {
        anyhow::bail!(
            "Alpaca account request failed | status={} | body={}",
            status,
            body
        );
    }


    // ============================================================
    // PARSE ACCOUNT
    // ============================================================

    let account =
        serde_json::from_str::<AlpacaAccountBalance>(
            &body
        )?;


    Ok(account)
}