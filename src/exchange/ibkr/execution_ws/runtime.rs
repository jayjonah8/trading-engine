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
        mpsc::{self, UnboundedReceiver},
        watch,
    },
    time::{sleep, Duration},
};

use crate::{
    exchange::ibkr::execution_ws::health::ExecutionWsHealth,
    shared::print_log,
};


// ============================================================
// IBKR EXECUTION WS
// ============================================================

/// Handle returned to the IBKR adapter.
///
/// The adapter consumes raw IBKR order updates through `raw_rx` and observes
/// execution-monitoring health through `health_rx`.
pub struct IbkrExecutionWs {
    pub raw_rx: UnboundedReceiver<SubscriptionItem<OrderUpdate>>,
    pub health_rx: watch::Receiver<ExecutionWsHealth>,
}


// ============================================================
// START EXECUTION WS
// ============================================================

/// Starts the persistent IBKR execution-monitoring runtime.
///
/// The spawned runtime owns:
///
/// - TWS / IB Gateway connection
/// - IBKR API handshake/authentication
/// - `order_update_stream()`
/// - health transitions
/// - reconnect attempts
///
/// The adapter remains responsible for mapping raw IBKR updates into
/// canonical Engine `ExecEvent`s.
pub fn start_execution_ws(
    address: String,
    client_id: i32,
    debug_mode: bool,
) -> IbkrExecutionWs {
    let (tx_raw, raw_rx) = mpsc::unbounded_channel::<SubscriptionItem<OrderUpdate>>();
    let (health_tx, health_rx) = watch::channel(ExecutionWsHealth::Connecting);

    tokio::spawn(async move {
        run_execution_ws(
            address,
            client_id,
            tx_raw,
            health_tx,
            debug_mode,
        )
        .await;
    });

    IbkrExecutionWs {
        raw_rx,
        health_rx,
    }
}


// ============================================================
// RUN EXECUTION WS
// ============================================================

async fn run_execution_ws(
    address: String,
    client_id: i32,
    tx_raw: mpsc::UnboundedSender<SubscriptionItem<OrderUpdate>>,
    health_tx: watch::Sender<ExecutionWsHealth>,
    debug_mode: bool,
) {
    // TWS can report Windows-style timezone names.
    //
    // Keep this registration at the IBKR connection boundary so every
    // reconnect uses the same timezone configuration.
    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    let mut reconnect_delay_secs = 1_u64;

    loop {
        // ========================================================
        // CONNECTING
        // ========================================================

        let _ = health_tx.send(ExecutionWsHealth::Connecting);

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
                let _ = health_tx.send(ExecutionWsHealth::Disconnected);

                eprintln!(
                    "[IBKR EXECUTION WS] CONNECTION FAILED | address={} | client_id={} | error={:?}",
                    address, client_id, error,
                );

                sleep(Duration::from_secs(reconnect_delay_secs)).await;

                reconnect_delay_secs = (reconnect_delay_secs * 2).min(10);

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
                let _ = health_tx.send(ExecutionWsHealth::Disconnected);

                eprintln!(
                    "[IBKR EXECUTION WS] ORDER UPDATE STREAM FAILED | error={:?}",
                    error,
                );

                client.disconnect().await;

                sleep(Duration::from_secs(reconnect_delay_secs)).await;

                reconnect_delay_secs = (reconnect_delay_secs * 2).min(10);

                continue;
            }
        };


        // ========================================================
        // READY
        // ========================================================

        reconnect_delay_secs = 1;

        let _ = health_tx.send(ExecutionWsHealth::Ready);

        print_log(
            debug_mode,
            true,
            "[IBKR EXECUTION WS] READY | stream=order_updates".to_string(),
        );


        // ========================================================
        // ORDER UPDATE LOOP
        // ========================================================

        while let Some(item) = order_stream.next().await {
            match item {
                // Forward both canonical IBKR order data and IBKR Notices.
                //
                // The mapper needs the complete SubscriptionItem because
                // broker rejections can arrive as SubscriptionItem::Notice.
                Ok(item) => {
                    if tx_raw.send(item).is_err() {
                        println!(
                            "[IBKR EXECUTION WS] raw update receiver closed"
                        );

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
        // DISCONNECTED
        // ========================================================

        let _ = health_tx.send(ExecutionWsHealth::Disconnected);

        eprintln!(
            "[IBKR EXECUTION WS] DISCONNECTED | reconnecting_in={}s",
            reconnect_delay_secs,
        );


        // Best-effort cleanup of the old connection before reconnecting.
        client.disconnect().await;


        // ========================================================
        // RECONNECT BACKOFF
        // ========================================================

        sleep(Duration::from_secs(reconnect_delay_secs)).await;

        reconnect_delay_secs = (reconnect_delay_secs * 2).min(10);
    }
}