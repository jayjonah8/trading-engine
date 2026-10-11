#![allow(dead_code)]

use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

use crate::{
    core::execution::{
        ExecCommand,
        ExecEvent,
        OrderType,
        Venue,
    },

    exchange::simulation::{
        commands::market_order::execute_market_order,
        SharedSimulationMarketState,
    },

    shared::print_log,
};


// ============================================================
// SIMULATION EXECUTION ADAPTER
// ============================================================

/// Executes canonical Engine orders against live market data without
/// submitting orders to a real exchange.
///
/// The adapter uses the same `ExecCommand` -> `ExecEvent` interface as
/// the live and backtest execution adapters.
///
/// Live market-data providers update `SharedSimulationMarketState`
/// independently through the master market-data dispatcher.
///
/// The adapter reads that shared state when executing market orders.
///
/// Initial implementation:
///
/// - market orders only
/// - immediate full fills
/// - latest live bid/ask quote
/// - BUY executes at ask
/// - SELL executes at bid
/// - optional stale quote validation
/// - no simulated fees yet
///
/// TODO:
/// - Implement limit orders.
/// - Implement stop orders.
/// - Implement order cancellation.
/// - Implement order replacement.
/// - Implement partial fills.
/// - Implement venue-specific fees.
/// - Implement slippage and execution latency.
/// - Implement liquidity-aware execution.
pub async fn run_simulation_adapter(
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_exec_evt: UnboundedSender<ExecEvent>,
    market_state: SharedSimulationMarketState,
    execution_venue: Venue,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        format!(
            "[SIMULATION EXECUTION] adapter started | venue={:?}",
            execution_venue,
        ),
    );

    // ============================================================
    // PROCESS EXECUTION COMMANDS
    // ============================================================

    while let Some(command) =
        rx_cmd.recv().await
    {
        match command {

            // ========================================================
            // SUBMIT ORDER
            // ========================================================

            ExecCommand::SubmitOrder { order } => {

                // ====================================================
                // VALIDATE EXECUTION VENUE
                // ====================================================

                // The execution router should only forward commands
                // associated with the configured execution venue.
                //
                // Reject mismatched orders rather than simulating
                // execution against an unintended venue.
                if order.venue != execution_venue {
                    let _ = tx_exec_evt.send(
                        ExecEvent::OrderRejected {
                            order_id: order.id,
                            reason: format!(
                                "Simulation execution venue mismatch | order_venue={:?} | adapter_venue={:?}",
                                order.venue,
                                execution_venue,
                            ),
                            ts_ms: order.created_ts_ms,
                        }
                    );

                    continue;
                }


                // ====================================================
                // MARKET ORDER
                // ====================================================

                if order.order_type == OrderType::Market {
                    execute_market_order(
                        order,
                        &tx_exec_evt,
                        &market_state,
                        debug_mode,
                    )
                    .await;

                    continue;
                }


                // ====================================================
                // UNSUPPORTED ORDER TYPE
                // ====================================================

                // TODO:
                // Implement limit orders.
                // Implement stop-market orders.
                // Implement stop-limit orders.
                //
                // Unsupported orders are rejected explicitly so the
                // Engine does not leave them pending indefinitely.
                let _ = tx_exec_evt.send(
                    ExecEvent::OrderRejected {
                        order_id: order.id,
                        reason: format!(
                            "Simulation execution currently supports market orders only | order_type={:?}",
                            order.order_type,
                        ),
                        ts_ms: order.created_ts_ms,
                    }
                );
            }


            // ========================================================
            // CANCEL ORDER
            // ========================================================

            ExecCommand::CancelOrder {
                venue: _,
                order_id,
            } => {

                // TODO:
                // Implement cancellation of resting simulation orders.
                //
                // Market orders currently fill immediately, so there
                // is no resting market order to cancel.
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[SIMULATION EXECUTION] CANCEL NOT IMPLEMENTED | order_id={}",
                        order_id,
                    ),
                );
            }


            // ========================================================
            // REPLACE ORDER
            // ========================================================

            ExecCommand::ReplaceOrder {
                venue: _,
                order_id,
                changes: _,
            } => {

                // TODO:
                // Implement modification of resting simulation orders.
                //
                // This will support limit-price changes, quantity
                // changes, and other canonical replacement requests.
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[SIMULATION EXECUTION] REPLACE NOT IMPLEMENTED | order_id={}",
                        order_id,
                    ),
                );
            }


            // ========================================================
            // BACKTEST SYNCHRONIZATION
            // ========================================================

            ExecCommand::BacktestBarrier {
                venue,
                done: _,
            } => {

                // Simulation mode processes continuous live market data
                // and does not use historical synchronization barriers.
                //
                // Dropping the sender prevents an accidental backtest
                // runner from waiting indefinitely for an acknowledgement.
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[SIMULATION EXECUTION] unexpected backtest barrier | venue={:?}",
                        venue,
                    ),
                );
            }
        }
    }


    // ============================================================
    // SHUTDOWN
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[SIMULATION EXECUTION] adapter shutting down | venue={:?}",
            execution_venue,
        ),
    );
}