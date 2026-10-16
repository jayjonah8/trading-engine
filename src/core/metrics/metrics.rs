#![allow(dead_code)]

// ============================================================
// ENGINE METRICS
// ============================================================

/// Tracks execution fees, completed trade statistics, and
/// equity performance metrics used to evaluate trading
/// strategies across live, simulation, and backtest modes.

#[derive(Debug, Clone, Default)]
pub struct Metrics {
    // ========================================================
    // EXECUTION FEES
    // ========================================================

    // Total confirmed entry execution fees.
    pub entry_fees: f64,

    // Total confirmed exit execution fees.
    pub exit_fees: f64,

    // ========================================================
    // COMPLETED TRADE STATISTICS
    // ========================================================

    pub winning_trades: u64,
    pub losing_trades: u64,
    pub breakeven_trades: u64,

    /// Sum of positive net PnL from completed trades.
    pub gross_profit: f64,

    /// Absolute sum of negative net PnL from completed trades.
    pub gross_loss: f64,

    // ========================================================
    // EQUITY STATISTICS
    // ========================================================

    /// Last sampled minute for equity metrics.
    pub last_equity_sample_minute: Option<i64>,

    /// Highest observed account equity.
    pub peak_equity: f64,

    /// Maximum observed peak-to-trough drawdown.
    pub max_drawdown_pct: f64,

    /// Previous equity sample for return calculations.
    pub previous_equity: Option<f64>,

    /// Number of periodic equity returns.
    pub return_count: u64,

    /// Running mean of periodic equity returns.
    pub mean_return: f64,

    /// Running sum of squared deviations (Welford).
    pub return_m2: f64,
}


impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total accumulated trading fees.
    pub fn total_fees(&self) -> f64 {
        self.entry_fees + self.exit_fees
    }

    // ========================================================
    // COMPLETED TRADE METRICS
    // ========================================================

    /// Records the final NET PnL of one completed trade.
    ///
    /// Call exactly once when a logical Trade becomes Closed.
    pub fn record_closed_trade(&mut self, net_pnl: f64) {
        if !net_pnl.is_finite() {
            return;
        }

        if net_pnl > 0.0 {
            self.winning_trades += 1;
            self.gross_profit += net_pnl;
        } else if net_pnl < 0.0 {
            self.losing_trades += 1;
            self.gross_loss += -net_pnl;
        } else {
            self.breakeven_trades += 1;
        }
    }

    pub fn total_completed_trades(&self) -> u64 {
        self.winning_trades
            + self.losing_trades
            + self.breakeven_trades
    }

    pub fn win_rate_pct(&self) -> f64 {
        let total = self.total_completed_trades();

        if total == 0 {
            return 0.0;
        }

        self.winning_trades as f64 / total as f64 * 100.0
    }

    pub fn loss_rate_pct(&self) -> f64 {
        let total = self.total_completed_trades();

        if total == 0 {
            return 0.0;
        }

        self.losing_trades as f64 / total as f64 * 100.0
    }

    /// Expected NET USD profit/loss per completed trade.
    pub fn expected_value(&self) -> f64 {
        let total = self.total_completed_trades();

        if total == 0 {
            return 0.0;
        }

        (self.gross_profit - self.gross_loss) / total as f64
    }

    // ========================================================
    // EQUITY METRICS
    // ========================================================

    /// Records an equity sample at a fixed historical interval.
    ///
    /// For example, once per minute or once per five minutes.
    ///
    /// Do not call once per tick: Sharpe would then depend on
    /// market-data event frequency rather than a fixed interval.
    pub fn record_equity(&mut self, equity: f64) {
        if !equity.is_finite() || equity <= 0.0 {
            return;
        }

        // Initialize peak equity from the first sample.
        if self.peak_equity <= 0.0 {
            self.peak_equity = equity;
        }

        // Update historical peak.
        self.peak_equity = self.peak_equity.max(equity);

        // Calculate drawdown relative to the highest equity.
        let drawdown_pct =
            (self.peak_equity - equity) / self.peak_equity * 100.0;

        self.max_drawdown_pct =
            self.max_drawdown_pct.max(drawdown_pct);

        // Calculate return relative to the previous sample.
        if let Some(previous) = self.previous_equity {
            let ret = equity / previous - 1.0;

            // Welford's online mean and variance calculation.
            self.return_count += 1;

            let n = self.return_count as f64;
            let delta = ret - self.mean_return;

            self.mean_return += delta / n;

            let delta2 = ret - self.mean_return;
            self.return_m2 += delta * delta2;
        }

        self.previous_equity = Some(equity);
    }

    /// Calculates the annualized Sharpe ratio, assuming a zero risk-free rate.
    ///
    /// Requires at least 24 hours of one-minute equity return samples.
    /// Returns 0.0 if insufficient samples are available or the
    /// calculation cannot produce a valid result.
    ///
    /// periods_per_year must match the equity sampling interval.
    ///
    /// Examples for a 24/7 market:
    /// - 1-minute samples: 525_600
    /// - 5-minute samples: 105_120
    /// - Hourly samples: 8_760
    ///
    /// For US equities, use trading-session periods instead.
    ///
    /// TODO:
    /// Make the minimum sample requirement configurable to support
    /// different sampling intervals and market schedules.
    pub fn sharpe_ratio(&self, periods_per_year: f64) -> f64 {

        // Require at least 24 hours of one-minute returns.
        const MIN_RETURN_SAMPLES: u64 = 24 * 60;

        if self.return_count < MIN_RETURN_SAMPLES {
            return 0.0;
        }

        if !periods_per_year.is_finite() || periods_per_year <= 0.0 {
            return 0.0;
        }

        let variance =
            self.return_m2 / (self.return_count - 1) as f64;

        if !variance.is_finite() || variance <= 0.0 {
            return 0.0;
        }

        let std_dev = variance.sqrt();

        self.mean_return / std_dev * periods_per_year.sqrt()
    }
}