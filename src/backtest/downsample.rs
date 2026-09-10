#![allow(dead_code)]

use std::{
    collections::HashMap,
    time::Duration,
};

use crate::backtest::types::HistoricalTrade;

// Reduces dense raw trade data using fixed time windows for each symbol.
// Only the last trade from each window is kept, preserving the latest price
// while greatly reducing the number of events the backtest must process.
pub(crate) fn downsample_trades(
    trades: Vec<HistoricalTrade>,
    interval: Duration,
) -> Vec<HistoricalTrade> {
    // A zero-sized interval means no downsampling.
    if interval.is_zero() {
        return trades;
    }

    let interval_ns = interval.as_nanos();

    // Stores the currently-active bucket for each symbol.
    //
    // Example:
    //
    // "AAPL" -> (bucket 12345, last AAPL trade)
    // "TSLA" -> (bucket 12345, last TSLA trade)
    //
    // As newer trades arrive inside the same bucket, we simply replace
    // the stored trade so the LAST trade in that bucket survives.
    let mut active_buckets:
        HashMap<String, (i128, HistoricalTrade)> = HashMap::new();

    let mut filtered = Vec::with_capacity(trades.len());

    for trade in trades {
        // Convert the historical timestamp into nanoseconds since Unix epoch.
        //
        // We use this only to determine which fixed time bucket
        // the trade belongs to. The actual HistoricalTrade keeps
        // its original precise timestamp.
        let timestamp_ns =
            (trade.timestamp.timestamp() as i128 * 1_000_000_000)
                + trade.timestamp.timestamp_subsec_nanos() as i128;

        let bucket_id = timestamp_ns / interval_ns as i128;

        match active_buckets.get_mut(&trade.symbol) {
            Some((current_bucket_id, current_trade)) => {
                if *current_bucket_id == bucket_id {
                    // Same symbol + same time bucket.
                    //
                    // Replace the previous trade so we keep the LAST
                    // trade that occurred during this window.
                    *current_trade = trade;
                } else {
                    // This symbol has entered a new time bucket.
                    //
                    // The previously stored trade is therefore the LAST
                    // trade from the previous bucket, so keep it.
                    let previous_trade =
                        std::mem::replace(current_trade, trade);

                    filtered.push(previous_trade);

                    *current_bucket_id = bucket_id;
                }
            }

            None => {
                // First trade we've seen for this symbol.
                active_buckets.insert(
                    trade.symbol.clone(),
                    (bucket_id, trade),
                );
            }
        }
    }

    // Every symbol will still have its final active bucket stored in
    // the HashMap, so add those final trades to the output.
    for (_, (_, trade)) in active_buckets {
        filtered.push(trade);
    }

    // Because each symbol is downsampled independently, pushing completed
    // buckets can temporarily disturb global ordering.
    //
    // Sort again so the BacktestEngine receives one globally chronological
    // market-event stream across every asset.
    filtered.sort_by_key(|trade| trade.timestamp);

    filtered
}