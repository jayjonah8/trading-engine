use crate::{
    core::events::{Quote, TapeTrade},
    data::live::synthetic::asset::SyntheticAsset,
};


// ============================================================
// SYNTHETIC MARKET GENERATOR
// ============================================================
//
// Generates deterministic synthetic market data for one asset.
//
// The generator maintains an evolving synthetic midpoint and produces:
//
//     - Quote
//     - TapeTrade
//
// These are the SAME canonical market-data types used by real providers.
// Nothing downstream needs to know that the data is synthetic.
//
// IMPORTANT:
//
// This generator creates MARKET DATA only.
//
// It does NOT:
//
//     - accept orders
//     - simulate fills
//     - simulate slippage
//     - maintain positions
//     - generate ExecEvent
//
// Those responsibilities belong to the execution simulator.
//
// TODO:
// - Add configurable market regimes:
//      Normal
//      TrendingUp
//      TrendingDown
//      HighVolatility
//      Crash
//      Illiquid
//
// - Add regime transitions during runtime.
//
// - Add correlated multi-asset price movement.
//
// - Add market-wide shocks.
//
// - Add spread widening during volatility spikes.
//
// - Add realistic intraday volatility and volume curves.
//
// - Add configurable trade-vs-quote event frequency.
//
// - Add temporary liquidity disappearance.
//
// - Add price gaps.
//
// - Add trade bursts for throughput/stress testing.
//
// - Add deterministic scripted scenarios for reproducing specific
//   strategy, risk, order, and engine edge cases.
//
// - Add a synthetic HISTORICAL data provider later so the backtester
//   can generate deterministic multi-symbol tape/bar datasets without
//   requiring downloaded historical data.
//
// ============================================================


// ============================================================
// GENERATED MARKET UPDATE
// ============================================================

#[derive(Debug, Clone)]
pub struct SyntheticMarketUpdate {
    pub quote: Quote,
    pub trade: TapeTrade,
}


// ============================================================
// GENERATOR
// ============================================================

#[derive(Debug)]
pub struct SyntheticMarketGenerator {
    asset: SyntheticAsset,
    mid_price: f64,
    rng_state: u64,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl SyntheticMarketGenerator {
    pub fn new(asset: SyntheticAsset) -> Self {
        Self {
            mid_price: asset.starting_price,
            rng_state: asset.seed.max(1),
            asset,
        }
    }


    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn symbol(&self) -> &str {
        &self.asset.symbol
    }

    pub fn mid_price(&self) -> f64 {
        self.mid_price
    }

    pub fn events_per_second(&self) -> u32 {
        self.asset.events_per_second
    }


    // ========================================================
    // MARKET UPDATE
    // ========================================================

    /// Advances the synthetic market by one step and returns a new
    /// canonical quote and tape trade.
    pub fn next_update(&mut self, timestamp_ms: i64) -> SyntheticMarketUpdate {
        self.advance_price();

        let spread = self.mid_price * (self.asset.spread_bps / 10_000.0);
        let half_spread = spread * 0.5;

        let bid_price = (self.mid_price - half_spread).max(0.000_001);
        let ask_price = self.mid_price + half_spread;

        let bid_size = self.random_range(self.asset.min_quote_size, self.asset.max_quote_size);
        let ask_size = self.random_range(self.asset.min_quote_size, self.asset.max_quote_size);

        let quote = Quote {
            symbol: self.asset.symbol.clone(),
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            timestamp_ms,
        };

        // Synthetic trades occur somewhere between the current best
        // bid and ask. This gives downstream strategies realistic
        // trade-price movement without inventing execution behavior.
        let trade_price = self.random_range(bid_price, ask_price);
        let trade_size = self.random_range(self.asset.min_trade_size, self.asset.max_trade_size);

        let trade = TapeTrade {
            symbol: self.asset.symbol.clone(),
            price: trade_price,
            size: trade_size,
            timestamp_ms,
        };

        SyntheticMarketUpdate { quote, trade }
    }


    // ========================================================
    // PRICE PROCESS
    // ========================================================

    /// Advances the midpoint using a small zero-drift stochastic move.
    ///
    /// `SyntheticAsset::volatility` is interpreted as approximate
    /// annualized volatility. It is scaled down according to the
    /// configured number of generated updates per second.
    fn advance_price(&mut self) {
        const SECONDS_PER_YEAR: f64 = 365.25 * 24.0 * 60.0 * 60.0;

        let updates_per_second = self.asset.events_per_second.max(1) as f64;
        let dt = 1.0 / (SECONDS_PER_YEAR * updates_per_second);

        // Approximate a centered random variable in [-1, 1].
        //
        // A Gaussian generator can be added later if more realistic
        // return distributions are required.
        let shock = (self.next_f64() * 2.0) - 1.0;
        let move_fraction = self.asset.volatility * dt.sqrt() * shock;

        self.mid_price *= 1.0 + move_fraction;

        // Prevent invalid/non-positive synthetic prices.
        self.mid_price = self.mid_price.max(0.000_001);
    }


    // ========================================================
    // DETERMINISTIC PRNG
    // ========================================================

    /// Small deterministic xorshift generator.
    ///
    /// This is intentionally not cryptographic. Its purpose is simply
    /// to produce fast, reproducible synthetic market sequences.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.rng_state;

        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;

        self.rng_state = x;
        x
    }

    fn next_f64(&mut self) -> f64 {
        self.next_u64() as f64 / u64::MAX as f64
    }

    fn random_range(&mut self, min: f64, max: f64) -> f64 {
        if max <= min {
            return min;
        }

        min + (max - min) * self.next_f64()
    }
}