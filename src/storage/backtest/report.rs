#![allow(dead_code)]

use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

use chrono::Utc;

use crate::core::backtest::BacktestResult;

use crate::shared::utils::{
    fmt_int,
    fmt_decimal,
    fmt_signed,
};


// ============================================================
// REPORT CONFIGURATION
// ============================================================

/// Number of decimal places displayed in backtest reports.
const DECIMAL_PRECISION: usize = 4;


// ============================================================
// BACKTEST REPORT
// ============================================================

/// Prints and saves a completed backtest result.
///
/// Each report receives a unique filename.
///
/// Existing reports are never overwritten.
///
/// Output directory:
///
///     misc/backtest_results/
pub fn save_backtest_report(
    result: &BacktestResult,
) -> io::Result<PathBuf> {

    // ========================================================
    // OUTPUT DIRECTORY
    // ========================================================

    let output_dir =
        PathBuf::from("misc/backtest_results");

    fs::create_dir_all(&output_dir)?;


    // ========================================================
    // UNIQUE FILENAME
    // ========================================================

    // Use the current UTC timestamp rather than historical
    // backtest time so repeated backtests get unique filenames.
    //
    // Microseconds help distinguish rapid consecutive runs.
    //
    // create_new(true) prevents accidental overwrites.

    let timestamp =
        Utc::now().format("%Y-%m-%d_%H-%M-%S-%f");

    let venue =
        format!("{:?}", result.execution_venue)
            .to_lowercase();

    let filename =
        format!("{timestamp}_{venue}.txt");

    let file_path =
        output_dir.join(filename);


    // ========================================================
    // FORMAT REPORT
    // ========================================================

    let report = format!(
        "\n\n\
============================================================
                     BACKTEST RESULTS
============================================================

Execution Venue:       {:?}
Total Symbols:         {}

------------------------------------------------------------
BACKTEST PERIOD
------------------------------------------------------------

Starting Date:         {}
Ending Date:           {}
Elapsed Calendar Days: {}

------------------------------------------------------------
ACCOUNT PERFORMANCE
------------------------------------------------------------

Starting Balance:      ${}
Ending Balance:        ${}
Ending Equity:         ${}

Realized PnL:          ${}
Unrealized PnL:        ${}
Net Unrealized PnL:    ${}

Total Return:          {}%

------------------------------------------------------------
PERFORMANCE METRICS
------------------------------------------------------------

Sharpe Ratio:          {}
Expected Value:        ${}

Win Rate:              {}%
Loss Rate:             {}%

Max Drawdown:          {}%

------------------------------------------------------------
EXECUTION FEES
------------------------------------------------------------

Entry Fees:            ${}
Exit Fees:             ${}
Total Fees:            ${}

------------------------------------------------------------
TRADING ACTIVITY
------------------------------------------------------------

Closed Trades:         {}
Open Trades:           {}
Active Trades:         {}

============================================================
",
        // ====================================================
        // CONFIGURATION
        // ====================================================

        result.execution_venue,
        fmt_int(result.total_symbols),


        // ====================================================
        // BACKTEST PERIOD
        // ====================================================

        result.starting_date,
        result.ending_date,
        fmt_decimal(result.total_trading_days, DECIMAL_PRECISION),


        // ====================================================
        // ACCOUNT PERFORMANCE
        // ====================================================

        fmt_decimal(result.starting_balance, DECIMAL_PRECISION),
        fmt_decimal(result.ending_balance, DECIMAL_PRECISION),
        fmt_decimal(result.ending_equity, DECIMAL_PRECISION),

        fmt_signed(result.realized_pnl, DECIMAL_PRECISION),
        fmt_signed(result.unrealized_pnl, DECIMAL_PRECISION),
        fmt_signed(result.net_unrealized_pnl, DECIMAL_PRECISION),

        fmt_signed(result.total_return_pct, DECIMAL_PRECISION),


        // ====================================================
        // PERFORMANCE METRICS
        // ====================================================

        fmt_decimal(result.sharpe_ratio, DECIMAL_PRECISION),
        fmt_signed(result.expected_value, DECIMAL_PRECISION),

        fmt_decimal(result.win_rate_pct, DECIMAL_PRECISION),
        fmt_decimal(result.loss_rate_pct, DECIMAL_PRECISION),

        fmt_decimal(result.max_drawdown_pct, DECIMAL_PRECISION),


        // ====================================================
        // EXECUTION FEES
        // ====================================================

        fmt_decimal(result.entry_fees, DECIMAL_PRECISION),
        fmt_decimal(result.exit_fees, DECIMAL_PRECISION),
        fmt_decimal(result.total_fees, DECIMAL_PRECISION),


        // ====================================================
        // TRADING ACTIVITY
        // ====================================================

        fmt_int(result.closed_trades),
        fmt_int(result.open_trades),
        fmt_int(result.active_trades),
    );


    // ========================================================
    // PRINT REPORT
    // ========================================================

    println!("{report}");


    // ========================================================
    // SAVE REPORT
    // ========================================================

    let mut file =
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file_path)?;

    file.write_all(
        report.as_bytes()
    )?;


    println!(
        "[BACKTEST] Results saved: {}",
        file_path.display()
    );

    Ok(file_path)
}