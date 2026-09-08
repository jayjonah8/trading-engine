// core/exec.rs (or in engine.rs if you prefer for now)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Long,
    Short,
}

// Sending trades to the exchange
#[derive(Debug, Clone)]
pub enum ExecCommand {
    /// Place a new market order on the exchange.
    /// Live Alpaca entries use dollar notional instead of a pre-calculated quantity.
    /// The actual filled quantity and average fill price come back through execution events.
    Place {
        /// Client-generated order id used to correlate the request with exchange events.
        cid: String,
        /// Symbol on the venue, e.g. "AAPL".
        symbol: String,
        /// Long = buy, Short = sell.
        side: Direction,
        /// Dollar amount to submit to the exchange, e.g. 100.0 = $100 notional.
        notional: f64,
    },

    /// Close an existing position on the exchange.
    /// The close is submitted as a market order against the existing position.
    Close {
        /// Client-generated id for the close request.
        cid: String,
        /// Symbol to close, e.g. "AAPL".
        symbol: String,
        /// None = close the entire position, Some(qty) = close only that quantity.
        qty: Option<f64>,
    },
}


// Receiving callback events after opening / closing trades.
#[derive(Debug, Clone)]
pub enum ExecEvent {
    /// REST order submission succeeded and the exchange created the order.
    Ack {
        /// Engine-generated client order ID used to correlate the order.
        cid: String,

        /// Symbol associated with the order, e.g. "AAPL".
        symbol: String,

        /// Exchange-generated Alpaca order ID.
        oid: String,
    },

    /// One execution occurred, but the opening order is not fully filled yet.
    OpenPartialFill {
        /// Engine-generated client order ID used to find the PendingOpen.
        cid: String,

        /// Symbol associated with the opening order.
        symbol: String,

        /// Price of this individual execution.
        fill_px: f64,

        /// Quantity filled by this individual execution only.
        fill_sz: f64,

        /// Cumulative average fill price across the entire order so far.
        avg_px: f64,

        /// Cumulative quantity filled across the entire order so far.
        total_sz: f64,

        /// Exchange-generated Alpaca order ID.
        oid: String,

        /// Unique Alpaca execution ID used to prevent duplicate accounting.
        execution_id: String,
    },

    /// Final execution occurred and the opening order is now fully filled.
    OpenFill {
        /// Engine-generated client order ID used to find the PendingOpen.
        cid: String,

        /// Symbol associated with the opening order.
        symbol: String,

        /// Price of this final individual execution.
        fill_px: f64,

        /// Quantity filled by this final individual execution only.
        fill_sz: f64,

        /// Final cumulative average fill price for the entire opening order.
        avg_px: f64,

        /// Final cumulative quantity filled for the entire opening order.
        total_sz: f64,

        /// Exchange-generated Alpaca order ID.
        oid: String,

        /// Unique Alpaca execution ID used to prevent duplicate accounting.
        execution_id: String,
    },

    /// One execution occurred, but the closing order is not fully filled yet.
    ClosePartialFill {
        /// Engine-generated client close order ID used to find the PendingClose.
        cid: String,

        /// Symbol associated with the closing order.
        symbol: String,

        /// Price of this individual close execution.
        fill_px: f64,

        /// Quantity closed by this individual execution only.
        fill_sz: f64,

        /// Cumulative average fill price across the closing order so far.
        avg_px: f64,

        /// Cumulative quantity filled across the closing order so far.
        total_sz: f64,

        /// Exchange-generated Alpaca order ID.
        oid: String,

        /// Unique Alpaca execution ID used to prevent duplicate accounting.
        execution_id: String,
    },

    /// Final execution occurred and the closing order is now fully filled.
    CloseFill {
        /// Engine-generated client close order ID used to find the PendingClose.
        cid: String,

        /// Symbol associated with the closing order.
        symbol: String,

        /// Price of this final individual close execution.
        fill_px: f64,

        /// Quantity closed by this final individual execution only.
        fill_sz: f64,

        /// Final cumulative average fill price for the entire closing order.
        avg_px: f64,

        /// Final cumulative quantity filled for the entire closing order.
        total_sz: f64,

        /// Exchange-generated Alpaca order ID.
        oid: String,

        /// Unique Alpaca execution ID used to prevent duplicate accounting.
        execution_id: String,
    },

    /// Exchange confirms the order is active but it has not fully filled yet.
    Resting {
        /// Engine-generated client order ID used to correlate the order.
        cid: String,

        /// Symbol associated with the order.
        symbol: String,

        /// Exchange-generated Alpaca order ID.
        oid: String,
    },

    /// A terminal execution or order-processing error occurred.
    Error {
        /// Engine-generated client order ID associated with the failed request.
        cid: String,

        /// Symbol associated with the failed request.
        symbol: String,

        /// Human-readable error returned by the adapter or exchange.
        message: String,
    },
}
