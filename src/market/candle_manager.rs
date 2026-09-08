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
// Every raw trade is passed through every configured timeframe.
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


    // Processes one raw trade through every configured timeframe.
    //
    // Returns any candles that became complete BEFORE this trade
    // is added to the new candle interval.
    pub fn on_trade(
        &mut self,
        symbol: &str,
        timestamp: DateTime<Utc>,
        price: f64,
        size: u64,
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

            // Then apply the current trade.
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


    // Used at the end of a backtest to close every candle
    // that is complete by the backtest end time.
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