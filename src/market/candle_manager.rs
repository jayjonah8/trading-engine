use chrono::{
    DateTime,
    Utc,
};

use crate::{
    core::events::Bar,

    market::{
        candle_aggregator::CandleAggregator,
        timeframe::Timeframe,
    },
};


// Owns all candle aggregators used by the bot.
//
// Every raw tape trade is passed through every configured timeframe.
pub struct CandleManager {
    aggregators:
        Vec<CandleAggregator>,
}

impl CandleManager {
    pub fn new(
        timeframes: &[Timeframe],
    ) -> Self {
        let aggregators =
            timeframes
                .iter()
                .copied()
                .map(
                    CandleAggregator::new
                )
                .collect();

        Self {
            aggregators,
        }
    }


    // Processes one raw tape trade through every configured timeframe.
    //
    // Returns any candles that became complete BEFORE this trade
    // is added to the new candle interval.
    //
    // `size` uses f64 because quantities may be fractional across
    // venues such as Hyperliquid and fractional-equity brokers.
    pub fn on_trade(
        &mut self,
        symbol: &str,
        timestamp: DateTime<Utc>,
        price: f64,
        size: f64,
    ) -> Vec<Bar> {
        let mut closed_bars =
            Vec::new();

        for aggregator in
            &mut self.aggregators
        {
            // Close completed candles first.
            closed_bars.extend(
                aggregator
                    .close_through(
                        timestamp
                    )
            );

            // Then apply the current tape trade.
            aggregator.on_trade(
                symbol,
                timestamp,
                price,
                size,
            );
        }

        // Gives deterministic ordering when several
        // timeframes close at the same timestamp.
        closed_bars.sort_by_key(
            |bar| {
                (
                    bar.end,
                    bar.timeframe,
                    bar.symbol.clone(),
                )
            }
        );

        closed_bars
    }


    // Closes every candle that is complete by the supplied timestamp.
    //
    // Useful for shutdown, historical replay, or any point where
    // completed candles need to be flushed without waiting for another trade.
    pub fn close_through(
        &mut self,
        timestamp: DateTime<Utc>,
    ) -> Vec<Bar> {
        let mut closed_bars =
            Vec::new();

        for aggregator in
            &mut self.aggregators
        {
            closed_bars.extend(
                aggregator
                    .close_through(
                        timestamp
                    )
            );
        }

        closed_bars.sort_by_key(
            |bar| {
                (
                    bar.end,
                    bar.timeframe,
                    bar.symbol.clone(),
                )
            }
        );

        closed_bars
    }
}