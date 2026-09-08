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


pub struct CandleHistory {
    retention: Duration,

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
            bars:
                HashMap::new(),
        }
    }


    // Adds one completed candle to its symbol + timeframe history.
    //
    // Old candles for that same symbol/timeframe are then removed
    // according to the configured retention period.
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

        prune_buffer(
            buf,
            now,
            self.retention,
        );
    }


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


fn prune_buffer(
    buf: &mut VecDeque<Bar>,
    now: DateTime<Utc>,
    retention: Duration,
) {
    let cutoff =
        now
            - chrono::Duration::nanoseconds(
                retention.as_nanos()
                    as i64
            );

    while let Some(bar) =
        buf.front()
    {
        if bar.end < cutoff {
            buf.pop_front();
        } else {
            break;
        }
    }
}