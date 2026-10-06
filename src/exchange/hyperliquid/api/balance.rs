#![allow(dead_code)]

use std::env;

use anyhow::{Context, Result};
use ethers::{
    signers::{LocalWallet, Signer},
    types::H160,
};

use hyperliquid_rust_sdk::{
    BaseUrl,
    InfoClient,
    UserStateResponse,
};

// ============================================================
// HYPERLIQUID MARGIN ACCOUNT BALANCES
// ============================================================

/// Fetches the current Hyperliquid margin account balances.
///
/// Returns:
/// (account_value, total_margin_used, total_ntl_pos, total_raw_usd, withdrawable)
pub async fn get_hyperliquid_account_balances() -> Result<(f64, f64, f64, f64, f64)> {
    let private_key = env::var("HYPERLIQUID_PRIVATE_KEY")
        .context("Missing HYPERLIQUID_PRIVATE_KEY env var")?;

    let wallet: LocalWallet = private_key.parse()
        .context("Invalid Hyperliquid private key")?;

    let user: H160 = wallet.address();

    let info_client = InfoClient::new(None, Some(BaseUrl::Mainnet))
        .await
        .context("Failed to create Hyperliquid InfoClient")?;

    let user_state: UserStateResponse = info_client.user_state(user)
        .await
        .context("Failed to fetch Hyperliquid user state")?;

    let account_value = user_state.margin_summary.account_value.parse::<f64>()
        .context("Failed to parse account_value")?;

    let total_margin_used = user_state.margin_summary.total_margin_used.parse::<f64>()
        .context("Failed to parse total_margin_used")?;

    let total_ntl_pos = user_state.margin_summary.total_ntl_pos.parse::<f64>()
        .context("Failed to parse total_ntl_pos")?;

    let total_raw_usd = user_state.margin_summary.total_raw_usd.parse::<f64>()
        .context("Failed to parse total_raw_usd")?;

    let withdrawable = user_state.withdrawable.parse::<f64>()
        .context("Failed to parse withdrawable")?;

    Ok((account_value, total_margin_used, total_ntl_pos, total_raw_usd, withdrawable))
}