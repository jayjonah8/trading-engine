#![allow(dead_code)]

use crate::core::config::Config;

use crate::notifications::{
    discord::send_discord_message,
    slack::send_slack_message,
};

/// Sends a remote notification to Slack, Discord, or both.
///
/// Notifications are best-effort and must never block the trading engine.
///
/// Rules:
/// - Does nothing during backtests.
/// - Sends to Slack only when Slack notifications are enabled
///   and a Slack channel ID was provided.
/// - Sends to Discord only when Discord notifications are enabled
///   and a Discord channel ID was provided.
/// - Slack and Discord network requests run in separate Tokio tasks,
///   so the trading engine never waits for either service.
/// - `None` can be passed for either channel to skip that service.
///
/// Example:
///
/// send_remote_message(
///     &cfg,
///     "AAPL trade opened",
///     Some(get_slack_trades_channel_id()),
///     Some(get_discord_trades_channel_id()),
/// );
pub fn send_remote_message(
    cfg: &Config,
    message: impl Into<String>,
    slack_channel_id: Option<String>,
    discord_channel_id: Option<u64>,
) {
    // Never create notification work during backtests.
    if cfg.enable_backtest_mode {
        return;
    }

    let message = message.into();

    if cfg.enable_slack_notifications {
        if let Some(channel_id) = slack_channel_id {
            let msg = message.clone();

            tokio::spawn(async move {
                send_slack_message(&channel_id, &msg).await;
            });
        }
    }

    if cfg.enable_discord_notifications {
        if let Some(channel_id) = discord_channel_id {
            tokio::spawn(async move {
                send_discord_message(channel_id, &message).await;
            });
        }
    }
}


