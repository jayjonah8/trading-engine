use chrono::{
    DateTime,
    Duration,
    Utc,
};

use crate::market::session::regular_market_session;


// ============================================================
// STRATEGY TRADING WINDOW
// ============================================================

/// Returns the time window during which a strategy is allowed
/// to open new trades for the supplied market timestamp.
///
/// The underlying regular market session is adjusted using:
///     - a delay after market open
///     - a buffer before market close
///
/// Returns None when regular_market_session() returns None,
/// including Saturdays and Sundays.
pub fn strategy_trading_window(
    timestamp: DateTime<Utc>,
    open_delay_minutes: i64,
    close_buffer_minutes: i64,
) -> Option<(
    DateTime<Utc>,
    DateTime<Utc>,
)> {
    let (
        session_start,
        session_end,
    ) = regular_market_session(
        timestamp
    )?;

    let trading_start =
        session_start
            + Duration::minutes(
                open_delay_minutes
            );

    let trading_end =
        session_end
            - Duration::minutes(
                close_buffer_minutes
            );

    Some((
        trading_start,
        trading_end,
    ))
}


// ============================================================
// REGULAR MARKET HOURS CHECK
// ============================================================

/// Returns true when the supplied timestamp is inside the
/// regular U.S. stock-market session.
///
/// Automatically returns false on Saturdays and Sundays because
/// regular_market_session() returns None for weekend dates.
pub fn is_regular_market_hours(
    timestamp: DateTime<Utc>,
) -> bool {
    let Some((
        session_start,
        session_end,
    )) = regular_market_session(
        timestamp
    ) else {
        return false;
    };

    timestamp >= session_start
        && timestamp < session_end
}




