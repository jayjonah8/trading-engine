use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
};

use dotenvy::dotenv;

use ethers::{
    signers::{
        LocalWallet,
        Signer,
    },
    types::H160,
};

use hyperliquid_rust_sdk::{
    BaseUrl,
    ExchangeClient,
    ExchangeDataStatus,
    ExchangeResponseStatus,
    InfoClient,
    MarketCloseParams,
    MarketOrderParams,
    Message,
    Subscription,
};

use tokio::{
    sync::mpsc::unbounded_channel,
    time::{
        sleep,
        Duration,
    },
};

use uuid::Uuid;


// ============================================================
// HYPERLIQUID MARKET ORDER + EXECUTION WS DIAGNOSTIC TEST
// ============================================================
//
// Run:
//
// cargo test test_hyperliquid_market_order_ws_updates -- --nocapture
//
// WARNING:
//
// This test uses Hyperliquid MAINNET and places a REAL market order.
//
// Flow:
//
// OrderUpdates WS ───────────────┐
//                               │
// UserEvents WS ────────────────┤
//                               │
//                               ↓
//                       subscriptions active
//                               ↓
//                       market BUY BTC
//                               ↓
//                       observe WS events
//                               ↓
//                           wait 5s
//                               ↓
//                     market CLOSE BTC
//                               ↓
//                       observe WS events
//                               ↓
//                    print all raw messages
//
// Purpose:
//
// This test is intentionally diagnostic.
//
// We want to learn exactly what Hyperliquid emits through:
//
// - Subscription::OrderUpdates
// - Subscription::UserEvents
//
// for:
//
// - market entry submission
// - immediate fill
// - market close
// - exit fill
//
// These raw messages will determine how `execution_ws.rs` and the future
// Hyperliquid execution mappers should translate venue events into canonical:
//
// - ExecEvent::OrderAccepted
// - ExecEvent::OrderWorking
// - ExecEvent::Fill
// - ExecEvent::OrderCanceled
// - ExecEvent::OrderRejected
// - ExecEvent::OrderReplaced
//
// `UserEvents` is especially important because it may contain the real
// individual execution/fill identity that should eventually become:
//
//     Fill.venue_execution_id
//
// rather than generating temporary synthetic execution IDs.


#[tokio::test]
async fn test_hyperliquid_market_order_ws_updates() {
    // ============================================================
    // ENV
    // ============================================================

    dotenv().ok();

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
    // WALLET
    // ============================================================

    let expected_user =
        H160::from_str(
            "0x75aAfefc48D23C65F49603491630308DcBd468f5"
        )
        .expect(
            "Invalid Hyperliquid wallet address"
        );


    let wallet_address =
        wallet.address();


    println!();
    println!("============================================================");
    println!("[TEST] HYPERLIQUID MARKET ORDER + WS");
    println!("============================================================");
    println!("[TEST] wallet_address={:?}", wallet_address);
    println!("[TEST] expected_user={:?}", expected_user);


    assert_eq!(
        wallet_address,
        expected_user,
        "HYPERLIQUID_PRIVATE_KEY does not match expected wallet address"
    );


    // ============================================================
    // EXCHANGE CLIENT
    // ============================================================

    // TODO:
    // Add cached Hyperliquid Meta later to avoid the extra metadata request
    // during ExchangeClient initialization.
    let exchange_client =
        ExchangeClient::new(
            None,
            wallet,
            Some(BaseUrl::Mainnet),
            None,
            None,
        )
        .await
        .expect(
            "Failed to create Hyperliquid ExchangeClient"
        );


    // ============================================================
    // ORDER UPDATES WS
    // ============================================================

    let mut order_info_client =
        InfoClient::new(
            None,
            Some(BaseUrl::Mainnet),
        )
        .await
        .expect(
            "Failed to create Hyperliquid OrderUpdates InfoClient"
        );


    let (
        order_sender,
        mut order_receiver,
    ) =
        unbounded_channel();


    let order_subscription_id =
        order_info_client
            .subscribe(
                Subscription::OrderUpdates {
                    user: expected_user,
                },
                order_sender,
            )
            .await
            .expect(
                "Failed to subscribe to Hyperliquid OrderUpdates"
            );


    println!(
        "[TEST] OrderUpdates subscribed | subscription_id={:?}",
        order_subscription_id,
    );


    // ============================================================
    // USER EVENTS WS
    // ============================================================

    let mut user_info_client =
        InfoClient::new(
            None,
            Some(BaseUrl::Mainnet),
        )
        .await
        .expect(
            "Failed to create Hyperliquid UserEvents InfoClient"
        );


    let (
        user_sender,
        mut user_receiver,
    ) =
        unbounded_channel();


    let user_subscription_id =
        user_info_client
            .subscribe(
                Subscription::UserEvents {
                    user: expected_user,
                },
                user_sender,
            )
            .await
            .expect(
                "Failed to subscribe to Hyperliquid UserEvents"
            );


    println!(
        "[TEST] UserEvents subscribed | subscription_id={:?}",
        user_subscription_id,
    );


    // ============================================================
    // WS MESSAGE COUNTERS
    // ============================================================

    let order_update_count =
        Arc::new(
            AtomicUsize::new(0)
        );

    let user_event_count =
        Arc::new(
            AtomicUsize::new(0)
        );


    // ============================================================
    // ORDER UPDATE RECEIVER
    // ============================================================

    let order_update_count_task =
        order_update_count.clone();


    let order_task =
        tokio::spawn(
            async move {
                while let Some(message) =
                    order_receiver.recv().await
                {
                    order_update_count_task
                        .fetch_add(
                            1,
                            Ordering::Relaxed,
                        );


                    println!();
                    println!("============================================================");
                    println!("[WS ORDER UPDATES]");
                    println!("============================================================");
                    println!("{:#?}", message);


                    match message {
                        Message::OrderUpdates(order_updates) => {
                            println!(
                                "[WS ORDER UPDATES PARSED] {:#?}",
                                order_updates,
                            );
                        }

                        other => {
                            println!(
                                "[WS ORDER UPDATES UNEXPECTED MESSAGE] {:#?}",
                                other,
                            );
                        }
                    }
                }
            }
        );


    // ============================================================
    // USER EVENT RECEIVER
    // ============================================================

    let user_event_count_task =
        user_event_count.clone();


    let user_task =
        tokio::spawn(
            async move {
                while let Some(message) =
                    user_receiver.recv().await
                {
                    user_event_count_task
                        .fetch_add(
                            1,
                            Ordering::Relaxed,
                        );


                    println!();
                    println!("============================================================");
                    println!("[WS USER EVENTS]");
                    println!("============================================================");
                    println!("{:#?}", message);


                    match message {
                        Message::User(user_event) => {
                            println!(
                                "[WS USER EVENT PARSED] {:#?}",
                                user_event,
                            );
                        }

                        other => {
                            println!(
                                "[WS USER EVENT UNEXPECTED MESSAGE] {:#?}",
                                other,
                            );
                        }
                    }
                }
            }
        );


    // Give both subscriptions a moment to settle before placing the order.
    sleep(
        Duration::from_secs(2)
    )
    .await;


    // ============================================================
    // MARKET OPEN
    // ============================================================

    let quantity =
        0.00013_f64;


    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT MARKET OPEN");
    println!("============================================================");
    println!("[TEST] asset=BTC");
    println!("[TEST] side=BUY");
    println!("[TEST] qty={}", quantity);

    // ============================================================
    // OPEN CLOID
    // ============================================================

    let open_cloid =
        Uuid::new_v4();

    println!(
        "[TEST] open_cloid={}",
        open_cloid,
    );


   let market_open_params =
    MarketOrderParams {
        asset: "BTC",
        is_buy: true,
        sz: quantity,
        px: None,
        slippage: Some(0.01),

        // Diagnostic correlation identity.
        //
        // We expect this exact UUID to come back through:
        //
        // - OrderUpdates.order.cloid
        // - UserEvents::Fills[].cloid
        cloid: Some(open_cloid),

        wallet: None,
    };


    let open_response =
        exchange_client
            .market_open(
                market_open_params
            )
            .await
            .expect(
                "Hyperliquid market_open request failed"
            );


    println!();
    println!(
        "[TEST] MARKET OPEN RAW RESPONSE:\n{:#?}",
        open_response,
    );


    // ============================================================
    // PARSE OPEN RESPONSE
    // ============================================================

    let open_response =
        match open_response {
            ExchangeResponseStatus::Ok(response) => {
                response
            }

            ExchangeResponseStatus::Err(error) => {
                panic!(
                    "Hyperliquid rejected market open: {:?}",
                    error,
                );
            }
        };


    let open_data =
        open_response
            .data
            .expect(
                "Market open response contained no data"
            );


    let open_status =
        open_data
            .statuses
            .first()
            .cloned()
            .expect(
                "Market open response contained no status"
            );


    match open_status {
        ExchangeDataStatus::Filled(order) => {
            println!(
                "[TEST] MARKET OPEN FILLED | {:#?}",
                order,
            );
        }

        ExchangeDataStatus::Resting(order) => {
            println!(
                "[TEST] MARKET OPEN RESTING | {:#?}",
                order,
            );
        }

        other => {
            panic!(
                "Unexpected market open status: {:?}",
                other,
            );
        }
    }


    // ============================================================
    // WAIT BEFORE CLOSE
    // ============================================================

    println!();
    println!(
        "[TEST] waiting 5 seconds before closing full BTC position..."
    );


    sleep(
        Duration::from_secs(5)
    )
    .await;


    // ============================================================
    // MARKET CLOSE
    // ============================================================

    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT MARKET CLOSE");
    println!("============================================================");
    println!("[TEST] asset=BTC");
    println!("[TEST] sz=None -> close full position");

    // ============================================================
    // CLOSE CLOID
    // ============================================================

    let close_cloid =
        Uuid::new_v4();

    println!(
        "[TEST] close_cloid={}",
        close_cloid,
    );


    let market_close_params =
        MarketCloseParams {
            asset: "BTC",

            // Close entire current position.
            sz: None,

            px: None,
            slippage: Some(0.01),

            // Separate client identity for the exit Order.
            cloid: Some(close_cloid),

            wallet: None,
        };


    let close_response =
        exchange_client
            .market_close(
                market_close_params
            )
            .await
            .expect(
                "Hyperliquid market_close request failed"
            );


    println!();
    println!(
        "[TEST] MARKET CLOSE RAW RESPONSE:\n{:#?}",
        close_response,
    );


    // ============================================================
    // PARSE CLOSE RESPONSE
    // ============================================================

    let close_response =
        match close_response {
            ExchangeResponseStatus::Ok(response) => {
                response
            }

            ExchangeResponseStatus::Err(error) => {
                panic!(
                    "Hyperliquid rejected market close: {:?}",
                    error,
                );
            }
        };


    let close_data =
        close_response
            .data
            .expect(
                "Market close response contained no data"
            );


    let close_status =
        close_data
            .statuses
            .first()
            .cloned()
            .expect(
                "Market close response contained no status"
            );


    match close_status {
        ExchangeDataStatus::Filled(order) => {
            println!(
                "[TEST] MARKET CLOSE FILLED | {:#?}",
                order,
            );
        }

        ExchangeDataStatus::Resting(order) => {
            println!(
                "[TEST] MARKET CLOSE RESTING | {:#?}",
                order,
            );
        }

        other => {
            panic!(
                "Unexpected market close status: {:?}",
                other,
            );
        }
    }


    // ============================================================
    // WAIT FOR FINAL WS EVENTS
    // ============================================================

    // Leave the subscriptions running after the close so delayed OrderUpdates
    // or UserEvents have time to arrive before the diagnostic test shuts down.
    println!();
    println!(
        "[TEST] waiting 10 seconds for final WebSocket events..."
    );


    sleep(
        Duration::from_secs(10)
    )
    .await;


    // ============================================================
    // SUMMARY
    // ============================================================

    let order_updates_seen =
        order_update_count
            .load(Ordering::Relaxed);

    let user_events_seen =
        user_event_count
            .load(Ordering::Relaxed);


    println!();
    println!("============================================================");
    println!("[TEST] WS SUMMARY");
    println!("============================================================");
    println!(
        "[TEST] OrderUpdates messages seen={}",
        order_updates_seen,
    );
    println!(
        "[TEST] UserEvents messages seen={}",
        user_events_seen,
    );


    // ============================================================
    // UNSUBSCRIBE
    // ============================================================

    order_info_client
        .unsubscribe(
            order_subscription_id
        )
        .await
        .expect(
            "Failed to unsubscribe OrderUpdates"
        );


    user_info_client
        .unsubscribe(
            user_subscription_id
        )
        .await
        .expect(
            "Failed to unsubscribe UserEvents"
        );


    // Receiver loops are diagnostic background tasks. Abort them explicitly
    // after the subscriptions have been shut down so the test cannot hang
    // waiting for channel closure behavior.
    order_task.abort();
    user_task.abort();


    // ============================================================
    // BASIC DIAGNOSTIC ASSERTIONS
    // ============================================================

    assert!(
        order_updates_seen > 0,
        "No Hyperliquid OrderUpdates messages were observed"
    );


    assert!(
        user_events_seen > 0,
        "No Hyperliquid UserEvents messages were observed"
    );


    println!();
    println!(
        "[TEST] Hyperliquid market-order WebSocket diagnostic completed."
    );
}