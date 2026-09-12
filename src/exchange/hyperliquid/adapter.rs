#![allow(dead_code)]

use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

use crate::{
    core::{
        config::Config,

        execution::{
            ExecCommand,
            ExecEvent,
            Venue,
        },
    },

    shared::print_log,
};


// ============================================================
// RUN HYPERLIQUID EXECUTION ADAPTER
// ============================================================

/// Runs the Hyperliquid execution adapter.
///
/// This adapter is the venue-specific boundary between the canonical trading
/// engine and Hyperliquid.
///
/// Flow:
///
/// Engine
///     -> ExecCommand
///     -> Execution Router
///     -> Hyperliquid Adapter
///     -> Hyperliquid API
///
/// Hyperliquid
///     -> Hyperliquid Adapter
///     -> ExecEvent
///     -> Engine
///
/// The Engine remains the authoritative owner of Order, Trade, Position,
/// FillLedger, and RiskState. This adapter must never directly mutate engine
/// state.
///
/// This initial implementation only establishes the canonical command loop.
/// Hyperliquid SDK execution and ExecEvent mapping will be added next.
pub async fn run_hyperliquid_exchange_adapter(
    _cfg: Config,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    _tx_evt: UnboundedSender<ExecEvent>,
) {
    let debug_mode = true;

    print_log(
        debug_mode,
        true,
        "[HYPERLIQUID ADAPTER] started",
    );


    // ============================================================
    // COMMAND LOOP
    // ============================================================

    while let Some(command) =
        rx_cmd.recv().await
    {
        match command {

            // ========================================================
            // SUBMIT ORDER
            // ========================================================

            ExecCommand::SubmitOrder {
                order,
            } => {
                // The execution router should only send Hyperliquid orders
                // to this adapter. Keep this defensive check so accidental
                // routing mistakes are visible during development.
                if order.venue != Venue::Hyperliquid {
                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[HYPERLIQUID ADAPTER] INVALID ROUTE | order_id={} | venue={:?}",
                            order.id,
                            order.venue,
                        ),
                    );

                    continue;
                }


                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID ADAPTER] SUBMIT ORDER | order_id={} | trade_id={:?} | strategy={} | symbol={} | side={:?} | type={:?} | qty={} | limit_px={:?} | stop_px={:?} | reduce_only={} | post_only={}",
                        order.id,
                        order.trade_id,
                        order.strategy_id,
                        order.symbol,
                        order.side,
                        order.order_type,
                        order.quantity,
                        order.limit_px,
                        order.stop_px,
                        order.reduce_only,
                        order.post_only,
                    ),
                );


                // TODO:
                // Translate the canonical Order into the appropriate
                // Hyperliquid SDK request.
                //
                // Examples:
                //
                // OrderType::Market
                //     -> marketable Hyperliquid order
                //
                // OrderType::Limit
                //     -> Hyperliquid limit order
                //
                // OrderType::StopMarket / StopLimit
                //     -> Hyperliquid trigger order behavior
                //
                // Keep all Hyperliquid-specific translation in this adapter
                // or its command_handler module rather than in Engine.


                // TODO:
                // Normalize quantity according to Hyperliquid's asset-specific
                // size precision before submitting the request.
                //
                // The Engine owns the desired canonical quantity.
                // Hyperliquid-specific rounding/formatting belongs here.


                // TODO:
                // Submit the translated order through ExchangeClient.


                // TODO:
                // Convert the immediate Hyperliquid response into canonical
                // ExecEvents.
                //
                // Expected mappings include:
                //
                // accepted/resting
                //     -> ExecEvent::OrderAccepted / OrderWorking
                //
                // immediate execution
                //     -> ExecEvent::OrderAccepted
                //     -> ExecEvent::Fill
                //
                // rejection
                //     -> ExecEvent::OrderRejected
                //
                // transport/internal failure
                //     -> ExecEvent::Error


                // TODO:
                // Do not assume an order is filled merely because the submit
                // request succeeded. Authoritative execution state must come
                // from confirmed Hyperliquid execution information.
            }


            // ========================================================
            // CANCEL ORDER
            // ========================================================

            ExecCommand::CancelOrder {
                venue,
                order_id,
            } => {
                if venue != Venue::Hyperliquid {
                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[HYPERLIQUID ADAPTER] INVALID CANCEL ROUTE | order_id={} | venue={:?}",
                            order_id,
                            venue,
                        ),
                    );

                    continue;
                }


                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID ADAPTER] CANCEL ORDER | order_id={}",
                        order_id,
                    ),
                );


                // TODO:
                // Resolve the Hyperliquid venue order identifier associated
                // with this canonical OrderId.
                //
                // Decide whether the adapter keeps a lightweight correlation
                // map or receives enough execution identity from the command.


                // TODO:
                // Submit the Hyperliquid cancel request.


                // TODO:
                // Map the confirmed result into:
                //
                // ExecEvent::OrderCanceled
                //
                // or:
                //
                // ExecEvent::Error / OrderRejected
                //
                // depending on the canonical event semantics we finalize.
            }


            // ========================================================
            // REPLACE ORDER
            // ========================================================

            ExecCommand::ReplaceOrder {
                venue,
                order_id,
                changes,
            } => {
                if venue != Venue::Hyperliquid {
                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[HYPERLIQUID ADAPTER] INVALID REPLACE ROUTE | order_id={} | venue={:?}",
                            order_id,
                            venue,
                        ),
                    );

                    continue;
                }


                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID ADAPTER] REPLACE ORDER | order_id={} | quantity={:?} | limit_px={:?} | stop_px={:?}",
                        order_id,
                        changes.quantity,
                        changes.limit_px,
                        changes.stop_px,
                    ),
                );


                // TODO:
                // Translate canonical OrderChanges into Hyperliquid's order
                // modification mechanism.


                // TODO:
                // If Hyperliquid implements the requested change as
                // cancel-and-replace, preserve correct canonical order
                // correlation and venue order identity.


                // TODO:
                // Map the confirmed venue result into:
                //
                // ExecEvent::OrderReplaced
                //
                // or the appropriate canonical error event.
            }
        }
    }


    // ============================================================
    // SHUTDOWN
    // ============================================================

    print_log(
        debug_mode,
        true,
        "[HYPERLIQUID ADAPTER] shutting down",
    );
}