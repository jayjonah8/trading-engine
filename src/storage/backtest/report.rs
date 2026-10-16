#![allow(dead_code)]

use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

use chrono::Utc;

use crate::core::backtest::BacktestResult;


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
        "\
============================================================
                     BACKTEST RESULTS
============================================================

Execution Venue:      {:?}
Total Symbols:        {}

------------------------------------------------------------
BACKTEST PERIOD
------------------------------------------------------------

Starting Date:        {}
Ending Date:          {}
Elapsed Calendar Days: {}

------------------------------------------------------------
ACCOUNT PERFORMANCE
------------------------------------------------------------

Starting Balance:     ${:.2}
Ending Balance:       ${:.2}
Ending Equity:        ${:.2}

Realized PnL:         ${:+.4}
Unrealized PnL:       ${:+.4}
Net Unrealized PnL:   ${:+.4}

Total Return:         {:+.2}%

------------------------------------------------------------
TRADING ACTIVITY
------------------------------------------------------------

Closed Trades:        {}
Open Trades:          {}
Active Trades:        {}

------------------------------------------------------------
EXECUTION FEES
------------------------------------------------------------

Entry Fees:           ${:.4}
Exit Fees:            ${:.4}
Total Fees:           ${:.4}

============================================================
",
        result.execution_venue,
        result.total_symbols,

        result.starting_date,
        result.ending_date,
        result.total_trading_days,

        result.starting_balance,
        result.ending_balance,
        result.ending_equity,

        result.realized_pnl,
        result.unrealized_pnl,
        result.net_unrealized_pnl,

        result.total_return_pct,

        result.closed_trades,
        result.open_trades,
        result.active_trades,

        result.entry_fees,
        result.exit_fees,
        result.total_fees,
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