#![allow(dead_code)]

use super::types::{
    AlpacaAccountBalance,
    AlpacaConfig,
};

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