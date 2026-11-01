#![allow(dead_code)]

use std::{
    collections::{
        HashMap,
        VecDeque,
    },
    time::Duration,
};

use chrono::{
    DateTime,
    Utc,
};

use crate::{
    core::events::Bar,
    market::timeframe::Timeframe,
};


// ============================================================
// CANDLE HISTORY
// ============================================================
//
// Stores completed candles for each symbol and timeframe.
//
// Maintains a configurable retention period and automatically
// removes expired candles through two pruning mechanisms:
//
// 1. Per-symbol pruning runs whenever a new candle is pushed,
//    removing candles older than the configured retention period
//    for that specific symbol/timeframe.
//
// 2. Optional global pruning periodically scans all symbols and
//    timeframes, including inactive assets, removing only candles
//    that have exceeded the configured retention period.
//    By default, global pruning runs once every 24 hours.
//
// The retention period determines WHICH candles are removed.
// The global pruning interval determines WHEN cleanup occurs.
//
// For example, with 48-hour retention and daily global pruning,
// the engine checks all symbols every 24 hours and removes only
// candles older than 48 hours.
//
// Global pruning uses market timestamps rather than wall-clock time,
// ensuring consistent behavior across Live, Paper, Simulation,
// and Backtest modes. Cleanup is triggered by completed candles.
//
// Global pruning can be disabled without affecting normal candle
// storage, retrieval, or per-symbol pruning.


pub struct CandleHistory {
    retention: Duration,

    // Optional interval for pruning all symbols/timeframes.
    // None disables global pruning.
    global_prune_interval: Option<Duration>,

    // Market timestamp of the last global pruning operation.
    last_global_prune: Option<DateTime<Utc>>,

    // symbol
    //   ↓
    // timeframe
    //   ↓
    // closed candles
    bars:
        HashMap<
            String,
            HashMap<
                Timeframe,
                VecDeque<Bar>,
            >,
        >,
}


impl CandleHistory {
    pub fn new(
        retention: Duration,
    ) -> Self {
        assert!(
            !retention.is_zero(),
            "candle retention cannot be zero"
        );

        Self {
            retention,

            // Default: global pruning once every 24 hours.
            global_prune_interval: Some(
                Duration::from_secs(24 * 60 * 60)
            ),

            last_global_prune: None,

            bars: HashMap::new(),
        }
    }


    // ============================================================
    // CONFIGURE GLOBAL PRUNING
    // ============================================================

    // Some(interval) enables periodic global pruning.
    // None disables global pruning completely.
    //
    // Existing per-symbol pruning remains enabled regardless.
    pub fn with_global_pruning(
        mut self,
        interval: Option<Duration>,
    ) -> Self {
        if let Some(duration) = interval {
            assert!(
                !duration.is_zero(),
                "global pruning interval cannot be zero"
            );
        }

        self.global_prune_interval = interval;
        self.last_global_prune = None;

        self
    }


    // ============================================================
    // PUSH COMPLETED CANDLE
    // ============================================================

    // Adds one completed candle to its symbol + timeframe history.
    //
    // Old candles for that same symbol/timeframe are then removed
    // according to the configured retention period.
    //
    // Optional global pruning runs periodically using market time.
    pub fn push(
        &mut self,
        bar: Bar,
    ) {
        let now =
            bar.end;

        let symbol =
            bar.symbol.clone();

        let timeframe =
            bar.timeframe;

        let timeframe_map =
            self.bars
                .entry(symbol)
                .or_insert_with(
                    HashMap::new
                );

        let buf =
            timeframe_map
                .entry(timeframe)
                .or_insert_with(
                    VecDeque::new
                );

        buf.push_back(bar);

        // Existing per-symbol/timeframe pruning.
        prune_buffer(
            buf,
            now,
            self.retention,
        );

        // Optional periodic pruning across all symbols/timeframes.
        self.maybe_prune_all(now);
    }


    // ============================================================
    // PERIODIC GLOBAL PRUNING
    // ============================================================

    // Checks whether global pruning is enabled and whether enough
    // market time has elapsed since the previous global prune.
    //
    // Invalid intervals are safely ignored without interrupting
    // normal candle processing or per-symbol pruning.
    fn maybe_prune_all(
        &mut self,
        now: DateTime<Utc>,
    ) {
        // Global pruning is disabled.
        let Some(interval) = self.global_prune_interval else {
            return;
        };

        // Safely convert the configured interval.
        let Ok(interval) = chrono::Duration::from_std(interval) else {
            return;
        };

        let should_prune = match self.last_global_prune {
            Some(last) => {
                now.signed_duration_since(last) >= interval
            }

            None => true,
        };

        if !should_prune {
            return;
        }

        self.prune_all(now);

        self.last_global_prune = Some(now);
    }


    // ============================================================
    // PRUNE ALL SYMBOLS AND TIMEFRAMES
    // ============================================================

    // Removes expired candles across every symbol/timeframe.
    //
    // Also removes empty timeframe buffers and symbol entries
    // to prevent inactive assets from occupying memory.
    fn prune_all(
        &mut self,
        now: DateTime<Utc>,
    ) {
        let retention = self.retention;

        self.bars.retain(|_, timeframes| {
            timeframes.retain(|_, buf| {
                prune_buffer(
                    buf,
                    now,
                    retention,
                );

                !buf.is_empty()
            });

            !timeframes.is_empty()
        });
    }


    // ============================================================
    // GET CANDLE HISTORY
    // ============================================================

    // Returns every stored candle for one symbol/timeframe.
    pub fn get(
        &self,
        symbol: &str,
        timeframe: Timeframe,
    ) -> Option<&VecDeque<Bar>> {
        self.bars
            .get(symbol)?
            .get(&timeframe)
    }


    // ============================================================
    // GET LATEST CANDLE
    // ============================================================

    // Returns the newest completed candle.
    pub fn latest(
        &self,
        symbol: &str,
        timeframe: Timeframe,
    ) -> Option<&Bar> {
        self.get(
            symbol,
            timeframe,
        )?
        .back()
    }


    // ============================================================
    // GET CANDLE COUNT
    // ============================================================

    // Number of stored candles for one symbol/timeframe.
    pub fn len(
        &self,
        symbol: &str,
        timeframe: Timeframe,
    ) -> usize {
        self.get(
            symbol,
            timeframe,
        )
        .map_or(
            0,
            VecDeque::len,
        )
    }
}


// ============================================================
// PRUNE CANDLE BUFFER
// ============================================================

// Removes expired candles from the front of the buffer.
//
// Invalid duration conversions or timestamp arithmetic are
// safely ignored rather than causing a runtime panic.
fn prune_buffer(
    buf: &mut VecDeque<Bar>,
    now: DateTime<Utc>,
    retention: Duration,
) {
    // Safely convert the retention duration.
    let Ok(retention) = chrono::Duration::from_std(retention) else {
        return;
    };

    // Safely calculate the expiration cutoff.
    let Some(cutoff) = now.checked_sub_signed(retention) else {
        return;
    };

    while let Some(bar) = buf.front() {
        if bar.end < cutoff {
            buf.pop_front();
        } else {
            break;
        }
    }
}