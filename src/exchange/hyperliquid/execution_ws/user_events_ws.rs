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
// USER EVENTS WS
// ============================================================

// Hyperliquid UserEvents is the authoritative source for individual
// executions / fills.
//
// From the real diagnostic test, UserEvents::Fills provides:
//
// TradeInfo {
//     coin,
//     side,
//     px,
//     sz,
//     time,
//     hash,
//     start_position,
//     dir,
//     closed_pnl,
//     oid,
//     cloid,
//     crossed,
//     fee,
//     fee_token,
//     tid,
// }
//
// Most importantly:
//
//     oid
//         = Hyperliquid venue order ID
//
//     tid
//         = stable venue execution / trade ID
//
// `tid` should become the canonical Fill's `venue_execution_id`.
//
// This stream will eventually feed:
//
// UserEvents::Fills
//     ↓
// mapper/user_event.rs
//     ↓
// mapper/fill.rs
//     ↓
// ExecEvent::Fill
//     ↓
// Engine::handle_exec_fill()


// ============================================================
// RUN USER EVENTS WS
// ============================================================

pub async fn run_user_events_ws(
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
                health.user_events_ready = false;
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
                        "[HYPERLIQUID USER EVENTS WS] CLIENT ERROR | error={:?} | retry={}s",
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
                    Subscription::UserEvents {
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
                            "[HYPERLIQUID USER EVENTS WS] SUBSCRIBE ERROR | error={:?} | retry={}s",
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
                health.user_events_ready = true;
            }
        );


        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID USER EVENTS WS] CONNECTED | subscription_id={:?}",
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
                Message::User(_) => {
                    if tx_raw.send(message).is_err() {
                        print_log(
                            debug_mode,
                            true,
                            "[HYPERLIQUID USER EVENTS WS] DOWNSTREAM CHANNEL CLOSED"
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
                            "[HYPERLIQUID USER EVENTS WS] UNEXPECTED MESSAGE | message={:?}",
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
                health.user_events_ready = false;
            }
        );


        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID USER EVENTS WS] DISCONNECTED | retry={}s",
                reconnect_delay_secs,
            ),
        );


        // TODO:
        // Send remote Slack/Discord disconnect notification.
        //
        // TODO:
        // Reconcile executions after reconnect because fills could have
        // occurred while UserEvents was unavailable.


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