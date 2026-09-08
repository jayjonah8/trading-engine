use chrono_tz::America::New_York;
use std::time::SystemTime;
use chrono::Utc;
use std::env;

/// Prints a debug message only when both:
///
/// 1. The strategy's master debug mode is enabled.
/// 2. The individual log is enabled.
///
/// This gives each strategy a master debug switch while still
/// allowing individual log messages to be turned on or off.
///
/// Example:
///
/// print_log(
///     self.debug_mode,
///     true,
///     "This log will print if debug_mode is true.",
/// );
pub fn print_log(
    debug_mode: bool,
    enabled: bool,
    message: impl std::fmt::Display,
) {
    if debug_mode && enabled {
        println!("{}", message);
    }
}


pub fn get_bot_name() -> String {
    env::var("BOT_NAME")
        .expect("Missing env: BOT_NAME")
}


pub fn get_local_timestamp() -> String {
    Utc::now().with_timezone(&New_York).format("%Y-%m-%d %I:%M:%S %p").to_string()
}


pub fn get_utc_timestamp() -> String {
    Utc::now()
        .format("%Y-%m-%d %I:%M:%S %p UTC")
        .to_string()
}


/// Formats the actual elapsed lifetime of a live/simulated trade.
///
/// Uses the trade's wall-clock entry time and the current system time,
/// automatically displaying the duration in seconds, minutes, hours,
/// or days. Returns "N/A" when no wall-clock entry time is available,
/// such as for backtest trades that use historical timestamps.
pub fn format_trade_duration(
    entry_wall_time: Option<SystemTime>,
) -> String {
    let Some(entry_time) =
        entry_wall_time
    else {
        return "N/A".to_string();
    };

    let Ok(duration) =
        SystemTime::now()
            .duration_since(entry_time)
    else {
        return "N/A".to_string();
    };

    let seconds =
        duration.as_secs_f64();

    if seconds >= 86_400.0 {
        format!("{:.1} days", seconds / 86_400.0)
    } else if seconds >= 3_600.0 {
        format!("{:.1} hours", seconds / 3_600.0)
    } else if seconds >= 60.0 {
        format!("{:.1} min", seconds / 60.0)
    } else {
        format!("{:.1} sec", seconds)
    }
}


/// Formats the historical elapsed lifetime of a backtest trade.
///
/// Uses the trade's historical open timestamp and historical close timestamp,
/// automatically displaying the duration in seconds, minutes, hours, or days.
pub fn format_backtest_trade_duration(
    opened_ms: i64,
    closed_ms: i64,
) -> String {
    let duration_ms =
        closed_ms.saturating_sub(opened_ms);

    let seconds =
        duration_ms as f64 / 1000.0;

    if seconds >= 86_400.0 {
        format!("{:.1} days", seconds / 86_400.0)
    } else if seconds >= 3_600.0 {
        format!("{:.1} hours", seconds / 3_600.0)
    } else if seconds >= 60.0 {
        format!("{:.1} min", seconds / 60.0)
    } else {
        format!("{:.1} sec", seconds)
    }
}