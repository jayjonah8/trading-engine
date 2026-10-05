#![allow(dead_code)]

use std::env;

use ethers::signers::{LocalWallet, Signer};

use hyperliquid_rust_sdk::{
    BaseUrl,
    ExchangeClient,
    ExchangeDataStatus,
    ExchangeResponseStatus,
    InfoClient,
    MarketCloseParams,
};

pub async fn close_all_positions() -> anyhow::Result<()> {
    let private_key = env::var("HYPERLIQUID_PRIVATE_KEY")
        .map_err(|error| anyhow::anyhow!("Missing HYPERLIQUID_PRIVATE_KEY env var: {}", error))?;

    let wallet: LocalWallet = private_key
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid Hyperliquid private key: {}", error))?;

    let address = wallet.address();

    let exchange_client = ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create Hyperliquid ExchangeClient: {}", error))?;

    let info_client = InfoClient::new(None, Some(BaseUrl::Mainnet))
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create Hyperliquid InfoClient: {}", error))?;

    let user_state = info_client
        .user_state(address)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to fetch Hyperliquid user state: {}", error))?;

    let symbols: Vec<String> = user_state
        .asset_positions
        .iter()
        .map(|position| position.position.coin.clone())
        .collect();

    if symbols.is_empty() {
        // TODO: Send Discord/Slack message: no open Hyperliquid positions found on startup.
        return Ok(());
    }

    let mut closed = Vec::new();
    let mut failed = Vec::new();

    for symbol in &symbols {
        let params = MarketCloseParams {
            asset: symbol.as_str(),
            sz: None,
            px: None,
            slippage: Some(0.01),
            cloid: None,
            wallet: None,
        };

        match exchange_client.market_close(params).await {
            Ok(ExchangeResponseStatus::Ok(ok)) => {
                let filled = ok
                    .data
                    .and_then(|data| data.statuses.first().cloned())
                    .is_some_and(|status| matches!(status, ExchangeDataStatus::Filled(_)));

                if filled {
                    closed.push(symbol.clone());

                    // TODO: Log successful Hyperliquid position close.
                } else {
                    failed.push(symbol.clone());

                    // TODO: Log Hyperliquid position close failure.
                }
            }
            Ok(ExchangeResponseStatus::Err(_)) | Err(_) => {
                failed.push(symbol.clone());

                // TODO: Log Hyperliquid position close failure.
            }
        }
    }

    if !failed.is_empty() {
        // TODO: Send Discord/Slack error message containing the successfully
        // closed positions and the positions that failed to close.
        anyhow::bail!(
            "Failed to close {} Hyperliquid position(s): {}",
            failed.len(),
            failed.join(", ")
        );
    }

    // TODO: Send Discord/Slack success message containing all positions closed
    // during startup cleanup.

    Ok(())
}

