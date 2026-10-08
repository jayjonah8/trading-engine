#![allow(dead_code)]

use ethers::{
    signers::{
        LocalWallet,
        Signer,
    },
};

use hyperliquid_rust_sdk::{
    BaseUrl,
    ExchangeClient,
    Message,
    Meta,
};

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

    exchange::hyperliquid::{
        commands::{
            handle_cancel_order,
            handle_replace_order,
            handle_submit_order,
        },

        correlation::HyperliquidOrderCorrelation,

        execution_ws::{
            health::wait_until_execution_ws_ready,
            start_execution_ws,
        },

        mapper::{
            handle_order_updates,
            handle_user_event,
        },
    },

    shared::print_log,
};

use std::sync::Arc;
use tokio::sync::RwLock;


// ============================================================
// RUN HYPERLIQUID EXECUTION ADAPTER
// ============================================================

/// Runs the Hyperliquid execution adapter.
///
/// The adapter owns:
///
/// - the Hyperliquid execution client
/// - canonical Engine execution commands routed to Hyperliquid
/// - execution-critical WebSocket infrastructure
/// - adapter-local order correlation
///
/// The adapter never mutates authoritative Engine state.
///
/// Venue-specific SDK responses and asynchronous WebSocket events are
/// translated into canonical `ExecEvent`s and sent back to the Engine.
///
/// Runtime flow:
///
///     Engine
///         ↓
///     ExecCommand
///         ↓
///     execution router
///         ↓
///     Hyperliquid adapter
///         │
///         ├── Submit / Cancel / Replace
///         │
///         └── execution WebSockets
///               ├── OrderUpdates
///               └── UserEvents
///                         ↓
///                      mapper
///                         ↓
///                     ExecEvent
///                         ↓
///                       Engine
///
/// The adapter keeps only transport/correlation state. Canonical Orders,
/// Trades, Positions, risk state, and accounting remain owned by the Engine.
pub async fn run_hyperliquid_adapter(
    _cfg: Config,
    meta_cache: Arc<RwLock<Meta>>,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
) {
    let debug_mode =
        true;


    // ============================================================
    // WALLET
    // ============================================================

    let private_key =
        std::env::var(
            "HYPERLIQUID_PRIVATE_KEY"
        )
        .expect(
            "HYPERLIQUID_PRIVATE_KEY env var missing"
        );


    let wallet: LocalWallet =
        private_key
            .parse()
            .expect(
                "Invalid HYPERLIQUID_PRIVATE_KEY"
            );


    // Capture the public wallet address before moving `wallet` into the
    // ExchangeClient.
    //
    // Hyperliquid execution WebSocket subscriptions are user-address scoped.
    let user =
        wallet.address();


    let meta =
        meta_cache
            .read()
            .await
            .clone();


    // ============================================================
    // EXCHANGE CLIENT
    // ============================================================

    let exchange_client =
        Arc::new(
            ExchangeClient::new(
                None,
                wallet,
                Some(BaseUrl::Mainnet),
                Some(meta),
                None,
            )
            .await
            .expect(
                "Failed to create Hyperliquid ExchangeClient"
            )
        );


    // ============================================================
    // EXECUTION WEBSOCKETS
    // ============================================================

    // Hyperliquid execution state is observed through two independent,
    // execution-critical user streams:
    //
    // OrderUpdates
    //     -> order lifecycle
    //
    // UserEvents
    //     -> actual individual executions / fills
    //
    // The adapter does not begin consuming execution commands until both
    // subscriptions have successfully connected.
    let mut execution_ws =
        start_execution_ws(
            user,
            debug_mode,
        );


    print_log(
        debug_mode,
        true,
        "[HYPERLIQUID ADAPTER] waiting for execution WebSockets",
    );


    if let Err(error) =
        wait_until_execution_ws_ready(
            &mut execution_ws.health_rx
        )
        .await
    {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ADAPTER] execution WebSocket health channel closed before readiness | error={}",
                error,
            ),
        );

        return;
    }


    print_log(
        debug_mode,
        true,
        "[HYPERLIQUID ADAPTER] execution WebSockets ready",
    );


    // ============================================================
    // ORDER CORRELATION
    // ============================================================

    // Hyperliquid receives the Engine's ClientOrderId as CLOID and returns
    // that CLOID through OrderUpdates and UserEvents.
    //
    // Correlation supports both directions:
    //
    // WebSocket path:
    //
    //     CLOID
    //       ↓
    //     ClientOrderId
    //       ↓
    //     OrderId
    //
    // Engine command path:
    //
    //     OrderId
    //       ↓
    //     HyperliquidOrderRef
    //       ├── ClientOrderId -> CLOID
    //       └── symbol        -> asset
    //
    // This is adapter-local transport correlation only.
    //
    // Authoritative canonical Order state remains entirely inside EngineState.
    let mut correlation =
        HyperliquidOrderCorrelation::new();


    print_log(
        debug_mode,
        true,
        "[HYPERLIQUID ADAPTER] started",
    );


    // ============================================================
    // ADAPTER EVENT LOOP
    // ============================================================

    // The adapter consumes two asynchronous input sources:
    //
    // 1. Engine commands
    //
    //     rx_cmd
    //         ↓
    //     Submit / Cancel / Replace
    //
    // 2. Hyperliquid execution events
    //
    //     execution_ws.raw_rx
    //         ↓
    //     mapper
    //         ↓
    //     ExecEvent
    //
    // Keeping correlation inside this task means the adapter remains the
    // single owner of its mutable transport state.
    //
    // Slow venue requests are spawned independently after extracting only
    // the transport information they require.
    loop {
        tokio::select! {

            // ====================================================
            // ENGINE COMMAND
            // ====================================================

            command =
                rx_cmd.recv() =>
            {
                let Some(command) =
                    command
                else {
                    // Engine/router command channel has closed.
                    //
                    // This is a normal shutdown condition for the adapter.
                    break;
                };


                match command {

                    // ================================================
                    // SUBMIT ORDER
                    // ================================================

                    ExecCommand::SubmitOrder {
                        order,
                    } => {
                        // --------------------------------------------
                        // ROUTE VALIDATION
                        // --------------------------------------------

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


                        // --------------------------------------------
                        // REGISTER CORRELATION
                        // --------------------------------------------

                        // Register correlation synchronously BEFORE spawning
                        // the venue request.
                        //
                        // Hyperliquid WS events may arrive immediately after
                        // submission, so the adapter must already be able to
                        // resolve:
                        //
                        //     CLOID
                        //       ↓
                        //     ClientOrderId
                        //       ↓
                        //     OrderId
                        //
                        // The reverse lookup is also registered so later
                        // Engine commands can resolve:
                        //
                        //     OrderId
                        //       ↓
                        //     ClientOrderId/CLOID + symbol
                        //
                        // This is adapter-local transport correlation only.
                        // Canonical Order state remains owned by the Engine.
                        correlation.insert(
                            order.id,
                            order.client_order_id,
                            order.symbol.clone(),
                        );


                        // --------------------------------------------
                        // SPAWN SUBMISSION
                        // --------------------------------------------

                        // Clone only the shared transport handles needed by
                        // the task.
                        //
                        // Canonical Engine state is NOT shared with this task.
                        let exchange_client =
                            Arc::clone(
                                &exchange_client
                            );

                        let tx_evt =
                            tx_evt.clone();


                        // Do not await venue submission inside the adapter
                        // event loop.
                        //
                        // Hyperliquid can emit OrderUpdates/UserEvents before
                        // the SDK request returns. Running submission
                        // independently keeps the adapter free to consume
                        // those WS messages immediately.
                        //
                        // This also allows multiple independent orders to be
                        // in flight concurrently.
                        tokio::spawn(
                            async move {
                                handle_submit_order(
                                    &exchange_client,
                                    &tx_evt,
                                    order,
                                    debug_mode,
                                )
                                .await;
                            }
                        );
                    }


                    // ================================================
                    // CANCEL ORDER
                    // ================================================

                    ExecCommand::CancelOrder {
                        venue,
                        order_id,
                    } => {
                        // --------------------------------------------
                        // ROUTE VALIDATION
                        // --------------------------------------------

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


                        // --------------------------------------------
                        // RESOLVE CORRELATION
                        // --------------------------------------------

                        // Resolve the adapter-local Hyperliquid order
                        // reference synchronously BEFORE spawning the
                        // asynchronous cancellation request.
                        //
                        // Cancellation begins with only the canonical:
                        //
                        //     OrderId
                        //
                        // Hyperliquid requires:
                        //
                        //     asset + CLOID
                        //
                        // Therefore:
                        //
                        //     OrderId
                        //         ↓
                        //     HyperliquidOrderRef
                        //         ├── ClientOrderId -> CLOID
                        //         └── symbol        -> asset
                        //
                        // Clone only this small transport reference into the
                        // spawned task.
                        //
                        // Correlation itself remains exclusively owned by this
                        // adapter event loop and does not require
                        // Arc<Mutex<_>> / Arc<RwLock<_>>.
                        let Some(order_ref) =
                            correlation
                                .get_by_order_id(
                                    order_id
                                )
                                .cloned()
                        else {
                            let ts_ms =
                                chrono::Utc::now()
                                    .timestamp_millis();


                            print_log(
                                debug_mode,
                                true,
                                format!(
                                    "[HYPERLIQUID ADAPTER] CANCEL CORRELATION NOT FOUND | order_id={}",
                                    order_id,
                                ),
                            );


                            let _ =
                                tx_evt.send(
                                    ExecEvent::Error {
                                        order_id:
                                            Some(order_id),

                                        message:
                                            format!(
                                                "Hyperliquid cancellation correlation not found for order_id={}",
                                                order_id,
                                            ),

                                        ts_ms,
                                    }
                                );


                            continue;
                        };


                        // --------------------------------------------
                        // SPAWN CANCELLATION
                        // --------------------------------------------

                        let exchange_client =
                            Arc::clone(
                                &exchange_client
                            );

                        let tx_evt =
                            tx_evt.clone();


                        // Do not block the adapter event loop while the
                        // cancellation request is in flight.
                        //
                        // This is particularly important for cancellation
                        // because an order may receive additional fills while
                        // cancellation is being processed.
                        //
                        // Those UserEvents / OrderUpdates must continue being
                        // consumed immediately.
                        tokio::spawn(
                            async move {
                                handle_cancel_order(
                                    &exchange_client,
                                    &tx_evt,
                                    order_id,
                                    order_ref,
                                    debug_mode,
                                )
                                .await;
                            }
                        );
                    }


                    // ================================================
                    // REPLACE ORDER
                    // ================================================

                    ExecCommand::ReplaceOrder {
                        venue,
                        order_id,
                        changes,
                    } => {
                        // --------------------------------------------
                        // ROUTE VALIDATION
                        // --------------------------------------------

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


                        // --------------------------------------------
                        // SPAWN REPLACEMENT
                        // --------------------------------------------

                        let exchange_client =
                            Arc::clone(
                                &exchange_client
                            );

                        let tx_evt =
                            tx_evt.clone();


                        // Replacement is still routed through its existing
                        // handler. Once Hyperliquid replacement is implemented,
                        // it can resolve the same adapter-local order reference
                        // pattern used by cancellation.
                        tokio::spawn(
                            async move {
                                handle_replace_order(
                                    &exchange_client,
                                    &tx_evt,
                                    order_id,
                                    changes,
                                    debug_mode,
                                )
                                .await;
                            }
                        );
                    }

                    
                    // ================================================
                    // BACKTEST SYNCHRONIZATION (LIVE ADAPTER)
                    // ================================================

                    ExecCommand::BacktestBarrier {
                        venue: _,
                        done,
                    } => {
                        // Backtest barriers should never reach live adapters.
                        // Drop the acknowledgment sender without signaling completion.
                        //
                        // This ensures an incorrectly routed barrier fails
                        // instead of falsely reporting successful synchronization.
                        drop(done);

                        print_log(
                            debug_mode,
                            true,
                            "[HYPERLIQUID ADAPTER] unexpected backtest barrier received",
                        );
                    }
                }
            }


            // ====================================================
            // HYPERLIQUID EXECUTION WS MESSAGE
            // ====================================================

            message =
                execution_ws.raw_rx.recv() =>
            {
                let Some(message) =
                    message
                else {
                    print_log(
                        debug_mode,
                        true,
                        "[HYPERLIQUID ADAPTER] execution WebSocket message channel closed",
                    );


                    // The raw channel should normally remain alive while the
                    // execution WS runtime is running.
                    //
                    // Treat channel closure as adapter shutdown rather than
                    // continuing to submit orders without execution events.
                    break;
                };


                match message {

                    // ================================================
                    // ORDER LIFECYCLE
                    // ================================================

                    Message::OrderUpdates(
                        order_updates
                    ) => {
                        // OrderUpdates is responsible for lifecycle facts such
                        // as:
                        //
                        // - working
                        // - canceled
                        // - other venue order statuses
                        //
                        // A "filled" order update does NOT create a canonical
                        // Fill here.
                        //
                        // Actual individual executions come from UserEvents.
                        handle_order_updates(
                            order_updates,
                            &correlation,
                            &tx_evt,
                            debug_mode,
                        );
                    }


                    // ================================================
                    // USER EVENTS / EXECUTIONS
                    // ================================================

                    Message::User(
                        user_event
                    ) => {
                        // UserEvents::Fills contains the authoritative
                        // individual Hyperliquid executions.
                        //
                        // mapper/user_event.rs dispatches each TradeInfo to
                        // mapper/fill.rs, which creates canonical Fill events.
                        //
                        // Multiple fills for the same OrderId naturally feed
                        // the Engine's generic fill accumulator, allowing:
                        //
                        //     Working
                        //        ↓
                        //     Fill
                        //        ↓
                        //     partial quantity
                        //        ↓
                        //     Fill
                        //        ↓
                        //     ...
                        //        ↓
                        //     total filled quantity == order quantity
                        //        ↓
                        //     Filled
                        handle_user_event(
                            user_event,
                            &correlation,
                            &tx_evt,
                            debug_mode,
                        );
                    }


                    // ================================================
                    // OTHER SDK MESSAGE
                    // ================================================

                    other => {
                        // The execution WS workers are intended to forward only
                        // the two subscription types above.
                        //
                        // Ignore unrelated SDK messages defensively rather than
                        // changing execution state based on an unknown message.
                        print_log(
                            debug_mode,
                            true,
                            format!(
                                "[HYPERLIQUID ADAPTER] IGNORED EXECUTION WS MESSAGE | message={:?}",
                                other,
                            ),
                        );
                    }
                }
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