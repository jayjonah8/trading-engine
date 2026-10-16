#![allow(dead_code)]

use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

use crate::{
    core::execution::{
        fill::allocate_execution_id,
        ExecCommand,
        ExecEvent,
        Fill,
        OrderType,
    },

    fees::calculate_execution_fee,

    exchange::backtest::SharedBacktestMarketState,

    shared::print_log,
};

// TODO:
// Your fee calculations are working, but your backtest currently fills market orders using the historical trade price.
// In actual trading:
// * Market buys generally execute at the ask.
// * Market sells generally execute at the bid.
// * Additional slippage may occur.
// * See the simulation adaptor for house the ask / bid logic works
// Therefore, your fee accounting is correct, but your backtest execution model may still produce optimistic results until you incorporate bid/ask spreads and slippage.


// ============================================================
// BACKTEST EXECUTION ADAPTER
// ============================================================

/// Executes canonical engine orders against historical market state.
///
/// The adapter intentionally uses the same `ExecCommand` -> `ExecEvent`
/// boundary as live execution adapters so the Engine processes simulated
/// executions through the normal canonical execution lifecycle.
///
/// Initial implementation:
///
/// - market orders only
/// - immediate full fills
/// - latest historical market price
/// - historical market timestamp
/// - no simulated fees yet
///
/// Limit orders, stop orders, partial fills, slippage, fees, cancellation,
/// and replacement behavior can be added without changing the Engine's
/// canonical execution interface.
///
/// Backtest synchronization uses `ExecCommand::BacktestBarrier` to ensure
/// previously queued execution commands have been processed before the
/// historical replay advances to the next market event.
pub async fn run_backtest_adapter(
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_exec_evt: UnboundedSender<ExecEvent>,
    market_state: SharedBacktestMarketState,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        "[BACKTEST EXECUTION] adapter started",
    );

    while let Some(command) =
        rx_cmd.recv().await
    {
        match command {
            ExecCommand::SubmitOrder { order } => {
                // ====================================================
                // MARKET ORDER
                // ====================================================

                if order.order_type != OrderType::Market {
                    let _ = tx_exec_evt.send(
                        ExecEvent::OrderRejected {
                            order_id: order.id,
                            reason: format!(
                                "Backtest execution currently supports market orders only | order_type={:?}",
                                order.order_type,
                            ),
                            ts_ms: order.created_ts_ms,
                        }
                    );

                    continue;
                }


                // ====================================================
                // CURRENT HISTORICAL MARKET PRICE
                // ====================================================

                let market_price = {
                    let state =
                        market_state.read().await;

                    state.get_price(
                        &order.symbol
                    )
                };

                let Some(market_price) =
                    market_price
                else {
                    let _ = tx_exec_evt.send(
                        ExecEvent::OrderRejected {
                            order_id: order.id,
                            reason: format!(
                                "No historical market price available for symbol={}",
                                order.symbol,
                            ),
                            ts_ms: order.created_ts_ms,
                        }
                    );

                    continue;
                };


                // ====================================================
                // ORDER ACCEPTED
                // ====================================================

                // Backtest execution does not have a real venue-side order ID.
                // Generate a deterministic runtime identifier so the canonical
                // Engine lifecycle remains identical to venue execution.
                let venue_order_id =
                    format!(
                        "BACKTEST-{}",
                        order.id
                    );

                if tx_exec_evt.send(
                    ExecEvent::OrderAccepted {
                        order_id: order.id,
                        venue_order_id,
                        ts_ms: market_price.ts_ms,
                    }
                ).is_err() {
                    break;
                }


                // ====================================================
                // CALCULATE BACKTEST EXECUTION FEE
                // ====================================================

                // Estimate maker/taker classification from OrderType.
                let is_maker = order.order_type.is_maker();

                // Calculate venue-specific estimated execution fee.
                //
                // The fee is calculated using the actual simulated
                // execution quantity and price.
                let execution_fee =
                    calculate_execution_fee(
                        order.venue,
                        order.quantity,
                        market_price.px,
                        is_maker,
                    );


                // ====================================================
                // FILL
                // ====================================================

                // V1 market execution model:
                //
                // - fill the entire requested quantity immediately
                // - use the latest historical market price
                // - preserve historical market time
                //
                // More realistic slippage, spread, liquidity, and partial-fill
                // behavior can be introduced later inside the simulator.
                let fill = Fill {
                    execution_id: allocate_execution_id(),
                    order_id: order.id,
                    venue: order.venue,
                    venue_execution_id: None,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    quantity: order.quantity,
                    price: market_price.px,
                    fee: Some(execution_fee),
                    ts_ms: market_price.ts_ms,
                };

                if tx_exec_evt.send(
                    ExecEvent::Fill {
                        fill,
                    }
                ).is_err() {
                    break;
                }


                // ====================================================
                // FILLED
                // ====================================================

                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[BACKTEST EXECUTION] MARKET FILL | order_id={} | symbol={} | side={:?} | qty={} | px={} | ts_ms={} | venue={:?}",
                        order.id,
                        order.symbol,
                        order.side,
                        order.quantity,
                        market_price.px,
                        market_price.ts_ms,
                        order.venue,
                    ),
                );
            }


            // ========================================================
            // CANCEL ORDER
            // ========================================================

            ExecCommand::CancelOrder {
                venue: _,
                order_id,
            } => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[BACKTEST EXECUTION] CANCEL NOT IMPLEMENTED | order_id={}",
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
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[BACKTEST EXECUTION] REPLACE NOT IMPLEMENTED | order_id={}",
                        order_id,
                    ),
                );
            }


            // ========================================================
            // BACKTEST SYNCHRONIZATION
            // ========================================================

            ExecCommand::BacktestBarrier {
                venue: _,
                done,
            } => {
                // Commands are processed sequentially by this adapter.
                //
                // Acknowledge only after all preceding execution commands
                // have been processed and their ExecEvents have been emitted.
                //
                // The historical runner must subsequently wait for its
                // ExecEvent barrier to ensure the Engine has applied fills.
                let _ = done.send(());
            }
        }
    }


    print_log(
        debug_mode,
        true,
        "[BACKTEST EXECUTION] adapter shutting down",
    );
}