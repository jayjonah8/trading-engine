#![allow(dead_code)]

use ethers::signers::LocalWallet;

use hyperliquid_rust_sdk::{
    BaseUrl,
    ExchangeClient,
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

    exchange::hyperliquid::commands::{
        handle_cancel_order,
        handle_replace_order,
        handle_submit_order,
    },

    shared::print_log,
};


// ============================================================
// RUN HYPERLIQUID EXECUTION ADAPTER
// ============================================================

/// Runs the Hyperliquid execution adapter.
///
/// The adapter owns the Hyperliquid execution client and consumes canonical
/// `ExecCommand`s routed from the Engine.
///
/// Venue-specific command translation lives in `commands/*`.
///
/// The adapter never mutates authoritative Engine state. Hyperliquid execution
/// results are translated into canonical `ExecEvent`s and sent back to the
/// Engine.
pub async fn run_hyperliquid_adapter(
    _cfg: Config,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
) {
    let debug_mode = true;


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


    // ============================================================
    // EXCHANGE CLIENT
    // ============================================================

    // TODO:
    // Fetch/cache Hyperliquid `Meta` once during startup and pass
    // `Some(meta)` here.
    //
    // Supplying cached Meta avoids the SDK performing an additional metadata
    // request during ExchangeClient initialization.
    //
    // `None` is acceptable for the initial implementation.
    let exchange_client =
        ExchangeClient::new(
            None,
            wallet,
            Some(BaseUrl::Mainnet),
            None, // TODO: add Some(meta),  
            None,
        )
        .await
        .expect(
            "Failed to create Hyperliquid ExchangeClient"
        ); 


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


                handle_submit_order(
                    &exchange_client,
                    &tx_evt,
                    order,
                    debug_mode,
                )
                .await;
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


                handle_cancel_order(
                    &exchange_client,
                    &tx_evt,
                    order_id,
                    debug_mode,
                )
                .await;
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


                handle_replace_order(
                    &exchange_client,
                    &tx_evt,
                    order_id,
                    changes,
                    debug_mode,
                )
                .await;
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