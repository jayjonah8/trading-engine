#![allow(dead_code)]

use futures::StreamExt;
use ibapi::{
    orders::OrderUpdate,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};
use tokio::{
    sync::{
        mpsc::UnboundedSender,
        watch,
    },
    time::{sleep, Duration},
};

use crate::{
    exchange::ibkr::execution_ws::health::ExecutionWsHealth,
    shared::print_log,
};


// ============================================================
// RECONNECT CONFIG
// ============================================================

const INITIAL_BACKOFF_SECS: u64 = 2;
const MAX_BACKOFF_SECS: u64 = 10;


// ============================================================
// RUN IBKR ORDER UPDATES
// ============================================================

/// Runs the persistent IBKR execution-monitoring supervisor.
///
/// Unlike Alpaca, IBKR does not expose a separate execution WebSocket.
///
/// `Client::connect()` establishes the persistent TWS / IB Gateway API
/// connection. `order_update_stream()` then exposes order lifecycle,
/// execution, commission, and notice events from that connection.
///
/// The supervisor:
///
/// 1. Connects to TWS / IB Gateway.
/// 2. Completes the IBKR API connection handshake.
/// 3. Creates `order_update_stream()`.
/// 4. Marks execution monitoring Ready.
/// 5. Forwards raw order updates to the adapter.
/// 6. Marks execution monitoring Disconnected if the stream fails.
/// 7. Reconnects automatically with exponential backoff.
///
/// Reconnection:
///
/// `1s -> 2s -> 4s -> 8s -> 10s -> 10s...`
pub(crate) async fn run_ibkr_order_updates(
    address: String,
    client_id: i32,
    tx_raw: UnboundedSender<SubscriptionItem<OrderUpdate>>,
    health_tx: watch::Sender<ExecutionWsHealth>,
    debug_mode: bool,
) {
    // TWS can report Windows-style timezone names.
    //
    // Keep timezone configuration at the IBKR connection boundary so every
    // fresh connection uses the same decoder configuration.
    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    let mut has_connected_once = false;

    loop {
        // ========================================================
        // CONNECTING
        // ========================================================

        set_execution_health(&health_tx, ExecutionWsHealth::Connecting);

        print_log(
            debug_mode,
            true,
            format!(
                "[IBKR EXECUTION WS] CONNECTING | address={} | client_id={}",
                address, client_id,
            ),
        );


        // ========================================================
        // CONNECT TO TWS / IB GATEWAY
        // ========================================================

        let client = match Client::connect(&address, client_id).await {
            Ok(client) => client,

            Err(error) => {
                set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);

                eprintln!(
                    "[IBKR EXECUTION WS] CONNECTION FAILED | address={} | client_id={} | error={:?} | retry={}s",
                    address, client_id, error, backoff_secs,
                );

                sleep_and_increase_backoff(&mut backoff_secs).await;
                continue;
            }
        };


        print_log(
            debug_mode,
            true,
            format!(
                "[IBKR EXECUTION WS] CONNECTED | address={} | client_id={}",
                address, client_id,
            ),
        );


        // ========================================================
        // CREATE ORDER UPDATE STREAM
        // ========================================================

        let mut order_stream = match client.order_update_stream().await {
            Ok(stream) => stream,

            Err(error) => {
                set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);

                eprintln!(
                    "[IBKR EXECUTION WS] ORDER UPDATE STREAM FAILED | error={:?} | retry={}s",
                    error, backoff_secs,
                );

                client.disconnect().await;

                sleep_and_increase_backoff(&mut backoff_secs).await;
                continue;
            }
        };


        // ========================================================
        // READY
        // ========================================================

        set_execution_health(&health_tx, ExecutionWsHealth::Ready);

        if has_connected_once {
            print_log(
                debug_mode,
                true,
                "[IBKR EXECUTION WS] RECONNECTED | stream=order_updates".to_string(),
            );
        } else {
            has_connected_once = true;
        }

        print_log(
            debug_mode,
            true,
            "[IBKR EXECUTION WS] READY | stream=order_updates".to_string(),
        );

        // Successful connection resets reconnect delay.
        backoff_secs = INITIAL_BACKOFF_SECS;


        // ========================================================
        // ORDER UPDATE LOOP
        // ========================================================

        while let Some(item) = order_stream.next().await {
            match item {
                // Forward both OrderUpdate data and IBKR Notices.
                //
                // The mapper receives the complete SubscriptionItem because
                // broker errors/rejections may arrive as Notice events.
                Ok(item) => {
                    if tx_raw.send(item).is_err() {
                        set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);

                        println!("[IBKR EXECUTION WS] raw update receiver closed");

                        client.disconnect().await;
                        return;
                    }
                }

                Err(error) => {
                    eprintln!(
                        "[IBKR EXECUTION WS] ORDER UPDATE STREAM ERROR | error={:?}",
                        error,
                    );

                    break;
                }
            }
        }


        // ========================================================
        // CONNECTION LOST
        // ========================================================

        set_execution_health(&health_tx, ExecutionWsHealth::Disconnected);

        eprintln!(
            "[IBKR EXECUTION WS] DISCONNECTED | reconnecting_in={}s",
            backoff_secs,
        );


        // ========================================================
        // CLEAN UP OLD CONNECTION
        // ========================================================

        client.disconnect().await;


        // ========================================================
        // RECONNECT BACKOFF
        // ========================================================

        sleep_and_increase_backoff(&mut backoff_secs).await;
    }
}


// ============================================================
// SET EXECUTION HEALTH
// ============================================================

fn set_execution_health(
    health_tx: &watch::Sender<ExecutionWsHealth>,
    health: ExecutionWsHealth,
) {
    let _ = health_tx.send(health);
}


// ============================================================
// RECONNECT BACKOFF
// ============================================================

/// `1s -> 2s -> 4s -> 8s -> 10s -> 10s...`
async fn sleep_and_increase_backoff(backoff_secs: &mut u64) {
    sleep(Duration::from_secs(*backoff_secs)).await;
    *backoff_secs = (*backoff_secs * 2).min(MAX_BACKOFF_SECS);
}