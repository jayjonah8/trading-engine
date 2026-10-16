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


// ============================================================
// NUMBER FORMATTING
// ============================================================

/// Adds thousands separators to a formatted number.
///
/// Examples:
/// 1000000       -> 1,000,000
/// 1234567.89    -> 1,234,567.89
/// -1234567.89   -> -1,234,567.89
/// +1234567.89   -> +1,234,567.89
pub fn add_commas(value: &str) -> String {
    let (sign, unsigned) = if let Some(rest) = value.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = value.strip_prefix('+') {
        ("+", rest)
    } else {
        ("", value)
    };

    let (integer, decimal) = match unsigned.split_once('.') {
        Some((integer, decimal)) => (integer, Some(decimal)),
        None => (unsigned, None),
    };

    let mut formatted = String::new();

    for (index, ch) in integer.chars().enumerate() {
        if index > 0 && (integer.len() - index) % 3 == 0 {
            formatted.push(',');
        }

        formatted.push(ch);
    }

    let mut result = format!("{sign}{formatted}");

    if let Some(decimal) = decimal {
        result.push('.');
        result.push_str(decimal);
    }

    result
}


/// Formats an integer with thousands separators.
///
/// Example:
/// 1234567 -> 1,234,567
pub fn fmt_int(value: impl std::fmt::Display) -> String {
    add_commas(&value.to_string())
}


/// Formats a floating-point number with commas.
///
/// Example:
/// 1234567.89123 -> 1,234,567.89
pub fn fmt_decimal(value: f64, precision: usize) -> String {
    add_commas(&format!("{value:.precision$}"))
}


/// Formats a floating-point number with an explicit sign.
///
/// Examples:
/// 1234567.89123  -> +1,234,567.89
/// -1234567.89123 -> -1,234,567.89
pub fn fmt_signed(value: f64, precision: usize) -> String {
    add_commas(&format!("{value:+.precision$}"))
}