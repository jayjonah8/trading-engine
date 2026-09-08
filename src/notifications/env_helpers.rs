#![allow(dead_code)]

use std::env;

use crate::core::config::Config;


// ============================================================
// SLACK CHANNEL IDs -> String
// ============================================================

pub fn get_slack_general_channel_id() -> String {
    env::var("SLACK_GENERAL_CHANNEL_ID")
        .expect("Missing env: SLACK_GENERAL_CHANNEL_ID")
}

pub fn get_slack_error_channel_id() -> String {
    env::var("SLACK_ERROR_CHANNEL_ID")
        .expect("Missing env: SLACK_ERROR_CHANNEL_ID")
}

pub fn get_slack_reconnect_error_channel_id() -> String {
    env::var("SLACK_RECONNECT_ERROR_CHANNEL_ID")
        .expect("Missing env: SLACK_RECONNECT_ERROR_CHANNEL_ID")
}

pub fn get_slack_trades_channel_id() -> String {
    env::var("SLACK_TRADES_CHANNEL_ID")
        .expect("Missing env: SLACK_TRADES_CHANNEL_ID")
}

pub fn get_slack_health_check_channel_id() -> String {
    env::var("SLACK_HEALTH_CHECK_CHANNEL_ID")
        .expect("Missing env: SLACK_HEALTH_CHECK_CHANNEL_ID")
}

pub fn get_slack_status_channel_id() -> String {
    env::var("SLACK_STATUS_CHANNEL_ID")
        .expect("Missing env: SLACK_STATUS_CHANNEL_ID")
}


// ============================================================
// DISCORD CHANNEL IDs -> u64
// ============================================================

pub fn get_discord_general_channel_id() -> u64 {
    env::var("DISCORD_GENERAL_CHANNEL_ID")
        .expect("Missing env: DISCORD_GENERAL_CHANNEL_ID")
        .parse()
        .expect("Invalid u64 in DISCORD_GENERAL_CHANNEL_ID")
}

pub fn get_discord_error_channel_id() -> u64 {
    env::var("DISCORD_ERROR_CHANNEL_ID")
        .expect("Missing env: DISCORD_ERROR_CHANNEL_ID")
        .parse()
        .expect("Invalid u64 in DISCORD_ERROR_CHANNEL_ID")
}

pub fn get_discord_reconnect_error_channel_id() -> u64 {
    env::var("DISCORD_RECONNECT_ERROR_CHANNEL_ID")
        .expect("Missing env: DISCORD_RECONNECT_ERROR_CHANNEL_ID")
        .parse()
        .expect("Invalid u64 in DISCORD_RECONNECT_ERROR_CHANNEL_ID")
}

pub fn get_discord_trades_channel_id() -> u64 {
    env::var("DISCORD_TRADES_CHANNEL_ID")
        .expect("Missing env: DISCORD_TRADES_CHANNEL_ID")
        .parse()
        .expect("Invalid u64 in DISCORD_TRADES_CHANNEL_ID")
}

pub fn get_discord_health_check_channel_id() -> u64 {
    env::var("DISCORD_HEALTH_CHECK_CHANNEL_ID")
        .expect("Missing env: DISCORD_HEALTH_CHECK_CHANNEL_ID")
        .parse()
        .expect("Invalid u64 in DISCORD_HEALTH_CHECK_CHANNEL_ID")
}

pub fn get_discord_status_channel_id() -> u64 {
    env::var("DISCORD_STATUS_CHANNEL_ID")
        .expect("Missing env: DISCORD_STATUS_CHANNEL_ID")
        .parse()
        .expect("Invalid u64 in DISCORD_STATUS_CHANNEL_ID")
}


// ============================================================
// .ENV VALIDATION CHECKS
// ============================================================

/// Validates all environment variables required by the enabled
/// remote notification services.
///
/// This should be called once when the bot starts.
///
/// Rules:
/// - Does nothing during backtests because remote notifications are disabled.
/// - If Slack notifications are enabled, verifies the Slack token and
///   every configured Slack channel ID.
/// - If Discord notifications are enabled, verifies the Discord token and
///   every configured Discord channel ID.
/// - Missing or invalid configuration intentionally panics so configuration
///   problems are discovered immediately at startup rather than while trading.
/// - Runtime Slack/Discord connection failures are handled by the individual
///   send functions and do not stop the trading bot.
///
/// Example:
///
/// validate_notification_env(&cfg);
pub fn validate_notification_env(cfg: &Config) {
    if cfg.enable_backtest_mode {
        return;
    }

    if cfg.enable_slack_notifications {
        env::var("SLACK_TOKEN")
            .expect("Missing env: SLACK_TOKEN");

        get_slack_general_channel_id();
        get_slack_error_channel_id();
        get_slack_reconnect_error_channel_id();
        get_slack_trades_channel_id();
        get_slack_health_check_channel_id();
        get_slack_status_channel_id();
    }

    if cfg.enable_discord_notifications {
        env::var("DISCORD_TOKEN")
            .expect("Missing env: DISCORD_TOKEN");

        get_discord_general_channel_id();
        get_discord_error_channel_id();
        get_discord_reconnect_error_channel_id();
        get_discord_trades_channel_id();
        get_discord_health_check_channel_id();
        get_discord_status_channel_id();
    }
}