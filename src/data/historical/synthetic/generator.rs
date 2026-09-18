#![allow(dead_code)]

use chrono::{
    DateTime,
    Duration,
    Utc,
};

use crate::{
    data::{
        historical::types::HistoricalTrade,
        live::synthetic::asset::SyntheticAsset,
    },
};


// ============================================================
// SYNTHETIC HISTORICAL GENERATOR
// ============================================================

/// Generates deterministic historical tape data for one synthetic asset.
///
/// Unlike the live synthetic provider, historical generation does not wait
/// for wall-clock time. The requested historical date range is generated as
/// quickly as possible and can therefore represent days, months, or years
/// of market activity without waiting in real time.
///
/// The same asset configuration, seed, start time, and end time should
/// always produce the same historical market.
///
/// TODO:
/// - Add configurable market regimes.
/// - Add trending / mean-reverting / crash regimes.
/// - Add spread and liquidity regimes.
/// - Add correlated multi-asset generation.
/// - Add market-wide shocks.
/// - Add trading-session/calendar filtering.
/// - Add configurable overnight/weekend behavior.
/// - Add deterministic scenario definitions.
/// - Add burst/high-frequency event periods.
/// - Add gaps and temporary liquidity disappearance.
/// - Consider chunked/streaming generation for extremely large datasets
///   instead of storing years of tick data in memory.
pub fn generate_historical_trades(
    asset: &SyntheticAsset,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<HistoricalTrade> {
    if end <= start {
        return Vec::new();
    }

    let events_per_second = asset.events_per_second.max(1);
    let interval_ns = 1_000_000_000i64 / i64::from(events_per_second);

    let mut timestamp = start;
    let mut price = asset.starting_price;
    let mut rng_state = asset.seed.max(1);

    let seconds_per_year = 365.25 * 24.0 * 60.0 * 60.0;
    let dt = (1.0 / events_per_second as f64) / seconds_per_year;

    let mut trades = Vec::new();

    while timestamp < end {
        let shock = (next_f64(&mut rng_state) * 2.0) - 1.0;
        let move_fraction = asset.volatility * dt.sqrt() * shock;

        price *= 1.0 + move_fraction;

        if !price.is_finite() || price <= 0.0 {
            price = asset.starting_price.max(0.000_000_01);
        }

        let size = random_range(
            &mut rng_state,
            asset.min_trade_size,
            asset.max_trade_size,
        );

        trades.push(
            HistoricalTrade {
                symbol: asset.symbol.clone(),
                timestamp,
                price,
                size,
            }
        );

        timestamp += Duration::nanoseconds(interval_ns);
    }

    trades
}


// ============================================================
// DETERMINISTIC PRNG
// ============================================================

fn next_u64(
    state: &mut u64,
) -> u64 {
    let mut x = *state;

    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;

    *state = x;

    x
}


fn next_f64(
    state: &mut u64,
) -> f64 {
    next_u64(state) as f64 / u64::MAX as f64
}


fn random_range(
    state: &mut u64,
    min: f64,
    max: f64,
) -> f64 {
    if max <= min {
        return min;
    }

    min + ((max - min) * next_f64(state))
}