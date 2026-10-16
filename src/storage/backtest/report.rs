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
TRADING ACTIVITY
------------------------------------------------------------

Closed Trades:         {}
Open Trades:           {}
Active Trades:         {}

------------------------------------------------------------
EXECUTION FEES
------------------------------------------------------------

Entry Fees:            ${}
Exit Fees:             ${}
Total Fees:            ${}

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
        fmt_decimal(result.total_trading_days, 1),


        // ====================================================
        // ACCOUNT PERFORMANCE
        // ====================================================

        fmt_decimal(result.starting_balance, 2),
        fmt_decimal(result.ending_balance, 2),
        fmt_decimal(result.ending_equity, 2),

        fmt_signed(result.realized_pnl, 4),
        fmt_signed(result.unrealized_pnl, 4),
        fmt_signed(result.net_unrealized_pnl, 4),

        fmt_signed(result.total_return_pct, 2),


        // ====================================================
        // TRADING ACTIVITY
        // ====================================================

        fmt_int(result.closed_trades),
        fmt_int(result.open_trades),
        fmt_int(result.active_trades),


        // ====================================================
        // EXECUTION FEES
        // ====================================================

        fmt_decimal(result.entry_fees, 4),
        fmt_decimal(result.exit_fees, 4),
        fmt_decimal(result.total_fees, 4),
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