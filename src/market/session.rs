use chrono::{
    DateTime,
    Datelike,
    TimeZone,
    Utc,
    Weekday,
};

use chrono_tz::America::New_York;


// ============================================================
// REGULAR U.S. MARKET SESSION
// ============================================================

/// Returns the regular U.S. stock-market session containing
/// the supplied market timestamp.
///
/// Regular market session:
///     9:30 AM -> 4:00 PM New York time.
///
/// Returns None when the supplied timestamp falls on:
///     - Saturday
///     - Sunday
///
/// America/New_York is used so daylight-saving time is handled
/// automatically for both EST and EDT.
///
/// Note: @todo
/// This currently handles normal weekdays and weekends only.
/// U.S. market holidays and early-close sessions can be added later.
/// The next upgrade later would be NYSE holidays and early-close days, 
/// but weekend handling is now centralized correctly in market/session.rs.
pub fn regular_market_session(
    timestamp: DateTime<Utc>,
) -> Option<(
    DateTime<Utc>,
    DateTime<Utc>,
)> {

    // ============================================================
    // CONVERT TO NEW YORK TIME
    // ============================================================

    // Market-session dates must be determined using New York time,
    // not UTC, because U.S. stock-market hours are based on ET.
    let ny_time =
        timestamp.with_timezone(
            &New_York
        );

    let date =
        ny_time.date_naive();


    // ============================================================
    // WEEKEND GATE
    // ============================================================

    // U.S. stock markets are closed on Saturdays and Sundays.
    //
    // Returning None here means every caller that depends on
    // regular_market_session() automatically rejects weekends.
    if matches!(
        date.weekday(),
        Weekday::Sat | Weekday::Sun
    ) {
        return None;
    }


    // ============================================================
    // SESSION START
    // ============================================================

    // Regular U.S. stock-market open:
    // 9:30 AM New York time.
    let session_start =
        New_York
            .with_ymd_and_hms(
                date.year(),
                date.month(),
                date.day(),
                9,
                30,
                0,
            )
            .single()?;


    // ============================================================
    // SESSION END
    // ============================================================

    // Regular U.S. stock-market close:
    // 4:00 PM New York time.
    let session_end =
        New_York
            .with_ymd_and_hms(
                date.year(),
                date.month(),
                date.day(),
                16,
                0,
                0,
            )
            .single()?;


    // ============================================================
    // RETURN UTC SESSION
    // ============================================================

    // Convert the New York session boundaries back to UTC so they
    // can be compared directly against historical/live UTC timestamps.
    Some((
        session_start
            .with_timezone(&Utc),

        session_end
            .with_timezone(&Utc),
    ))
}

