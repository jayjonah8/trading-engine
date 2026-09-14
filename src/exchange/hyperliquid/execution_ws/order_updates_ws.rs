#![allow(dead_code)]

use ethers::types::H160;

use hyperliquid_rust_sdk::{
    BaseUrl,
    InfoClient,
    Message,
    Subscription,
};

use tokio::{
    sync::{
        mpsc::UnboundedSender,
        watch,
    },
    time::{
        sleep,
        Duration,
    },
};

use crate::{
    exchange::hyperliquid::execution_ws::health::ExecutionWsHealth,
    shared::print_log,
};


// ============================================================
// ORDER UPDATES WS
// ============================================================

// Hyperliquid OrderUpdates is responsible for ORDER LIFECYCLE information.
//
// From the real diagnostic test:
//
// status = "open"
//     -> order became active at Hyperliquid
//
// status = "filled"
//     -> order lifecycle reports fully filled
//
// Other statuses will later map into:
//     - canceled
//     - rejected
//     - expired
//     - replaced / modified where applicable
//
// IMPORTANT:
//
// OrderUpdates should NOT be the authoritative source for individual Fill
// objects.
//
// Actual individual executions come through UserEvents::Fills because that
// stream provides:
//     - tid
//     - px
//     - sz
//     - fee
//     - execution timestamp
//
// Those fields are required for rigorous canonical Fill accounting.


// ============================================================
// RUN ORDER UPDATES WS
// ============================================================

pub async fn run_order_updates_ws(
    user: H160,
    tx_raw: UnboundedSender<Message>,
    health_tx: watch::Sender<ExecutionWsHealth>,
    debug_mode: bool,
) {
    let mut reconnect_delay_secs =
        1_u64;


    loop {
        // ============================================================
        // MARK DISCONNECTED
        // ============================================================

        health_tx.send_modify(
            |health| {
                health.order_updates_ready = false;
            }
        );


        // ============================================================
        // CREATE INFO CLIENT
        // ============================================================

        let mut info_client =
            match InfoClient::new(
                None,
                Some(BaseUrl::Mainnet),
            )
            .await
        {
            Ok(client) => {
                client
            }

            Err(error) => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID ORDER UPDATES WS] CLIENT ERROR | error={:?} | retry={}s",
                        error,
                        reconnect_delay_secs,
                    ),
                );

                // TODO:
                // Send remote Slack/Discord reconnect/error notification.

                sleep(
                    Duration::from_secs(
                        reconnect_delay_secs
                    )
                )
                .await;

                reconnect_delay_secs =
                    (reconnect_delay_secs * 2)
                        .min(30);

                continue;
            }
        };


        // ============================================================
        // SUBSCRIPTION CHANNEL
        // ============================================================

        let (
            sender,
            mut receiver,
        ) =
            tokio::sync::mpsc::unbounded_channel();


        // ============================================================
        // SUBSCRIBE
        // ============================================================

        let subscription_id =
            match info_client
                .subscribe(
                    Subscription::OrderUpdates {
                        user,
                    },
                    sender,
                )
                .await
            {
                Ok(subscription_id) => {
                    subscription_id
                }

                Err(error) => {
                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[HYPERLIQUID ORDER UPDATES WS] SUBSCRIBE ERROR | error={:?} | retry={}s",
                            error,
                            reconnect_delay_secs,
                        ),
                    );

                    // TODO:
                    // Send remote Slack/Discord reconnect/error notification.

                    sleep(
                        Duration::from_secs(
                            reconnect_delay_secs
                        )
                    )
                    .await;

                    reconnect_delay_secs =
                        (reconnect_delay_secs * 2)
                            .min(30);

                    continue;
                }
            };


        // ============================================================
        // READY
        // ============================================================

        reconnect_delay_secs =
            1;


        health_tx.send_modify(
            |health| {
                health.order_updates_ready = true;
            }
        );


        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ORDER UPDATES WS] CONNECTED | subscription_id={:?}",
                subscription_id,
            ),
        );


        // TODO:
        // Send remote recovery notification if this was a reconnection.


        // ============================================================
        // RECEIVE
        // ============================================================

        while let Some(message) =
            receiver.recv().await
        {
            match message {
                Message::OrderUpdates(_) => {
                    if tx_raw.send(message).is_err() {
                        print_log(
                            debug_mode,
                            true,
                            "[HYPERLIQUID ORDER UPDATES WS] DOWNSTREAM CHANNEL CLOSED"
                                .to_string(),
                        );

                        return;
                    }
                }

                other => {
                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[HYPERLIQUID ORDER UPDATES WS] UNEXPECTED MESSAGE | message={:?}",
                            other,
                        ),
                    );
                }
            }
        }


        // ============================================================
        // DISCONNECTED
        // ============================================================

        health_tx.send_modify(
            |health| {
                health.order_updates_ready = false;
            }
        );


        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID ORDER UPDATES WS] DISCONNECTED | retry={}s",
                reconnect_delay_secs,
            ),
        );


        // TODO:
        // Send remote Slack/Discord disconnect notification.
        //
        // TODO:
        // After reconnect support is fully wired, reconciliation should check
        // whether any order lifecycle events were missed while disconnected.


        sleep(
            Duration::from_secs(
                reconnect_delay_secs
            )
        )
        .await;


        reconnect_delay_secs =
            (reconnect_delay_secs * 2)
                .min(30);
    }
}