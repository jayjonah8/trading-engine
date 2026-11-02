#![allow(dead_code)]

use std::{
    collections::HashMap,
    time::Duration,
};

use crate::data::historical::types::HistoricalTrade;


// ============================================================
// STATEFUL HISTORICAL TRADE DOWNSAMPLER
// ============================================================

// Reduces dense historical trade data using fixed time windows
// independently for each symbol.
//
// Unlike downsample_trades(), this implementation preserves
// active buckets across multiple historical data chunks.
//
// Only the last trade from each time bucket is retained.
//
// This allows historical backtests to process large datasets
// in smaller chunks without resetting downsampling state.
//
// The same downsampler instance must be reused throughout
// the entire backtest.
pub(crate) struct StatefulTradeDownsampler {
    interval: Duration,

    // Stores the currently active bucket and its latest trade
    // independently for each symbol.
    active_buckets: HashMap<String, (i128, HistoricalTrade)>,
}


// ============================================================
// IMPLEMENTATION
// ============================================================

impl StatefulTradeDownsampler {

    // ============================================================
    // INITIALIZE
    // ============================================================

    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            active_buckets: HashMap::new(),
        }
    }


    // ============================================================
    // PROCESS HISTORICAL CHUNK
    // ============================================================

    // Processes chronologically ordered historical trades.
    //
    // Completed buckets are returned while unfinished buckets
    // remain in memory for the next chunk.
    //
    // The input must be sorted chronologically across all symbols.
    //
    // A zero interval disables downsampling.
    pub fn process_chunk(
        &mut self,
        trades: Vec<HistoricalTrade>,
    ) -> Vec<HistoricalTrade> {

        if self.interval.is_zero() {
            return trades;
        }

        let interval_ns = self.interval.as_nanos() as i128;

        let mut filtered = Vec::new();

        for trade in trades {
            let timestamp_ns =
                (trade.timestamp.timestamp() as i128 * 1_000_000_000)
                    + trade.timestamp.timestamp_subsec_nanos() as i128;

            let bucket_id = timestamp_ns.div_euclid(interval_ns);

            match self.active_buckets.get_mut(&trade.symbol) {

                Some((current_bucket_id, current_trade)) => {

                    if *current_bucket_id == bucket_id {
                        // Keep the last trade in the current bucket.
                        *current_trade = trade;

                    } else {
                        // The previous bucket is complete.
                        let previous_trade =
                            std::mem::replace(current_trade, trade);

                        filtered.push(previous_trade);

                        *current_bucket_id = bucket_id;
                    }
                }

                None => {
                    self.active_buckets.insert(
                        trade.symbol.clone(),
                        (bucket_id, trade),
                    );
                }
            }
        }

        // Completed buckets may be emitted out of order
        // because symbols are downsampled independently.
        //
        // Sort only by timestamp, matching downsample.rs.
        filtered.sort_by_key(|trade| trade.timestamp);

        filtered
    }


    // ============================================================
    // FINALIZE DOWNSAMPLING
    // ============================================================

    // Called only after the entire historical backtest has
    // finished loading its final chunk.
    //
    // Releases the final retained trade for each symbol.
    pub fn finish(&mut self) -> Vec<HistoricalTrade> {

        let mut remaining = Vec::new();

        for (_, (_, trade)) in self.active_buckets.drain() {
            remaining.push(trade);
        }

        // Sort only by timestamp, matching downsample.rs.
        remaining.sort_by_key(|trade| trade.timestamp);

        remaining
    }


    // ============================================================
    // FLUSH COMPLETED BUCKETS
    // ============================================================

    // Releases trades from buckets that have completely ended
    // before or at the supplied timestamp.
    //
    // This is useful at historical chunk boundaries because a
    // symbol might not produce another trade for several chunks.
    //
    // Buckets overlapping the timestamp remain active.
    pub fn flush_completed_through(
        &mut self,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Vec<HistoricalTrade> {

        if self.interval.is_zero() {
            return Vec::new();
        }

        let interval_ns = self.interval.as_nanos() as i128;

        let timestamp_ns =
            (timestamp.timestamp() as i128 * 1_000_000_000)
                + timestamp.timestamp_subsec_nanos() as i128;

        let completed_symbols: Vec<String> = self.active_buckets
            .iter()
            .filter_map(|(symbol, (bucket_id, _))| {
                let bucket_end = (*bucket_id + 1) * interval_ns;

                if bucket_end <= timestamp_ns {
                    Some(symbol.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut completed = Vec::with_capacity(completed_symbols.len());

        for symbol in completed_symbols {
            if let Some((_, trade)) = self.active_buckets.remove(&symbol) {
                completed.push(trade);
            }
        }

        // Sort only by timestamp, matching downsample.rs.
        completed.sort_by_key(|trade| trade.timestamp);

        completed
    }
}