#![allow(dead_code)]

use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::StreamExt;

use ibapi::{
    prelude::*,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};

use tokio::time::timeout;

// ============================================================
// GET IBKR ACCOUNT BALANCE
// ============================================================

/// Fetches the account's NetLiquidation value from IBKR.
///
/// Establishes a temporary connection to TWS / IB Gateway,
/// requests the account summary, and disconnects.
///
/// NetLiquidation represents total account equity, including
/// cash and open positions.
///
/// TODO:
/// Add account_id filtering when supporting multiple IBKR accounts.
/// Currently returns the first USD NetLiquidation value received.
/// The account ID should eventually come from Config or an environment
/// variable to ensure the correct trading account is selected.
pub async fn get_ibkr_account_balances(
    address: &str,
    client_id: i32,
    timezone_name: &str,
    timezone_alias: &str,
) -> Result<f64> {
    // ============================================================
    // TIMEZONE
    // ============================================================

    // Register before connecting because the IBKR connection
    // handshake may contain the configured TWS timezone.
    register_timezone_alias(timezone_name, timezone_alias);

    // ============================================================
    // CONNECT
    // ============================================================

    let client = Client::connect(address, client_id).await
        .context("Failed to connect to IBKR")?;

    // ============================================================
    // REQUEST ACCOUNT SUMMARY
    // ============================================================

    let group = AccountGroup("All".to_string());
    let tags = ["NetLiquidation"];

    let result: Result<f64> = async {
        let mut subscription = client.account_summary(&group, &tags).await
            .context("Failed to request IBKR account summary")?;

        let balance = timeout(Duration::from_secs(30), async {
            while let Some(item) = subscription.next().await {
                match item {
                    Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(summary))) => {
                        if summary.tag == "NetLiquidation"
                            && summary.currency == "USD"
                        {
                            let value = summary.value.parse::<f64>()
                                .context("Failed to parse IBKR NetLiquidation")?;

                            return Ok(value);
                        }
                    }

                    Ok(SubscriptionItem::Data(AccountSummaryResult::End)) => {
                        break;
                    }

                    Ok(SubscriptionItem::Notice(notice)) => {
                        if notice.is_error() {
                            bail!(
                                "IBKR account summary error | code={} | message={}",
                                notice.code,
                                notice.message
                            );
                        }
                    }

                    Err(error) => {
                        bail!("IBKR account summary stream error: {error}");
                    }
                }
            }

            bail!("IBKR NetLiquidation not received");
        })
        .await
        .context("Timed out fetching IBKR account balance")??;

        drop(subscription);

        Ok(balance)
    }
    .await;

    // ============================================================
    // DISCONNECT
    // ============================================================

    client.disconnect().await;

    result
}