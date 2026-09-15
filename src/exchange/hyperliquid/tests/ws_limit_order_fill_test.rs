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
    ClientLimit,
    ClientOrder,
    ClientOrderRequest,
    ExchangeClient,
    ExchangeDataStatus,
    ExchangeResponseStatus,
    InfoClient,
    Message,
    Subscription,
};

use tokio::{
    sync::{
        mpsc::unbounded_channel,
        watch,
    },
    time::{
        sleep,
        timeout,
        Duration,
    },
};

use uuid::Uuid;


// ============================================================
// HYPERLIQUID LIMIT ENTRY + LIMIT EXIT + WS DIAGNOSTIC TEST
// ============================================================
//
// Run:
//
// cargo test test_hyperliquid_limit_order_fill_ws_updates -- --nocapture
//
// WARNING:
//
// This test uses Hyperliquid MAINNET and places REAL orders.
//
// PURPOSE:
//
// Test the complete FILLED limit-order lifecycle:
//
//     AllMids WS
//         │
//         ▼
//     current BTC mid
//         │
//         ▼
//     marketable GTC BUY limit
//         │
//         ├── REST order response
//         ├── OrderUpdates
//         └── UserEvents fill
//         │
//         ▼
//     BTC position opened
//         │
//         ▼
//     refresh current BTC mid
//         │
//         ▼
//     marketable REDUCE-ONLY GTC SELL limit
//         │
//         ├── REST order response
//         ├── OrderUpdates
//         └── UserEvents fill
//         │
//         ▼
//     position closed
//
// ------------------------------------------------------------
// WHY "MARKETABLE" LIMIT ORDERS?
// ------------------------------------------------------------
//
// A limit order does NOT necessarily mean an order must rest.
//
// For a BUY:
//
//     limit price >= best ask
//
// can execute immediately.
//
// For a SELL:
//
//     limit price <= best bid
//
// can execute immediately.
//
// We only have AllMids in this diagnostic rather than the exact best
// bid/ask, so we deliberately cross the current mid by 0.50%:
//
//     ENTRY BUY:
//         mid * 1.005
//
//     EXIT SELL:
//         mid * 0.995
//
// This gives the order enough price tolerance to behave like an
// immediately executable limit while still exercising Hyperliquid's
// ClientOrder::Limit path.
//
// ------------------------------------------------------------
// IMPORTANT ENGINE BEHAVIOR
// ------------------------------------------------------------
//
// REST submission is NOT treated as authoritative execution.
//
// Actual execution is observed through UserEvents::Fills.
//
// OrderUpdates tells us about the order lifecycle.
//
// Therefore this diagnostic helps verify the architecture:
//
//     OrderUpdates -> order state
//     UserEvents   -> execution/fill state
//
// This distinction is particularly important for:
//     - partial fills
//     - multiple fills
//     - resting limits
//     - race conditions between REST and WS
//     - later market making.


#[tokio::test]
async fn test_hyperliquid_limit_order_fill_ws_updates() {
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
    println!("[TEST] HYPERLIQUID LIMIT ENTRY + LIMIT EXIT + WS");
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
            "Failed to create OrderUpdates InfoClient"
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
                "Failed to subscribe to OrderUpdates"
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
            "Failed to create UserEvents InfoClient"
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
                "Failed to subscribe to UserEvents"
            );


    println!(
        "[TEST] UserEvents subscribed | subscription_id={:?}",
        user_subscription_id,
    );


    // ============================================================
    // ALL MIDS WS
    // ============================================================

    // Keep price discovery on its own InfoClient.
    //
    // This lets the diagnostic independently observe:
    //
    //     market data
    //     order lifecycle
    //     executions
    //
    // which mirrors the separation we want in the production system.
    let mut mids_info_client =
        InfoClient::new(
            None,
            Some(BaseUrl::Mainnet),
        )
        .await
        .expect(
            "Failed to create AllMids InfoClient"
        );


    let (
        mids_sender,
        mut mids_receiver,
    ) =
        unbounded_channel();


    let mids_subscription_id =
        mids_info_client
            .subscribe(
                Subscription::AllMids,
                mids_sender,
            )
            .await
            .expect(
                "Failed to subscribe to AllMids"
            );


    println!(
        "[TEST] AllMids subscribed | subscription_id={:?}",
        mids_subscription_id,
    );


    // ============================================================
    // SHARED LATEST MID
    // ============================================================

    // A watch channel is convenient here because we only care about the
    // newest BTC mid rather than every historical mid update.
    let (
        tx_mid,
        mut rx_mid,
    ) =
        watch::channel::<Option<f64>>(
            None
        );


    // ============================================================
    // COUNTERS
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
                                "[WS ORDER UPDATES UNEXPECTED] {:#?}",
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
                                "[WS USER EVENT UNEXPECTED] {:#?}",
                                other,
                            );
                        }
                    }
                }
            }
        );


    // ============================================================
    // ALL MIDS RECEIVER
    // ============================================================

    let mids_task =
        tokio::spawn(
            async move {
                while let Some(message) =
                    mids_receiver.recv().await
                {
                    if let Message::AllMids(all_mids) =
                        message
                    {
                        if let Some(mid) =
                            all_mids.data.mids.get("BTC")
                        {
                            match mid.parse::<f64>() {
                                Ok(mid) => {
                                    let _ =
                                        tx_mid.send(
                                            Some(mid)
                                        );
                                }

                                Err(error) => {
                                    println!(
                                        "[WS ALL MIDS] failed parsing BTC mid={} | error={:?}",
                                        mid,
                                        error,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        );


    // ============================================================
    // WAIT FOR CURRENT BTC MID
    // ============================================================

    println!();
    println!(
        "[TEST] waiting for current BTC mid..."
    );


    let current_mid =
        timeout(
            Duration::from_secs(10),
            async {
                loop {
                    if let Some(mid) =
                        *rx_mid.borrow()
                    {
                        break mid;
                    }

                    rx_mid
                        .changed()
                        .await
                        .expect(
                            "BTC mid watch channel closed"
                        );
                }
            },
        )
        .await
        .expect(
            "Timed out waiting for BTC AllMids price"
        );


    println!(
        "[TEST] current BTC mid={}",
        current_mid,
    );


    // ============================================================
    // ENTRY PARAMETERS
    // ============================================================

    let quantity =
        0.00019_f64;


    // Make the BUY limit marketable.
    //
    // We intentionally permit execution up to 0.50% above the observed mid.
    //
    // Example:
    //
    //     mid = 77,200
    //
    //     raw limit = 77,586
    //
    // Hyperliquid has price precision rules, so for this BTC diagnostic
    // round the price to a whole dollar.
    let entry_limit_px =
        (current_mid * 1.005)
            .ceil();


    let entry_cloid =
        Uuid::new_v4();


    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT MARKETABLE LIMIT ENTRY");
    println!("============================================================");
    println!("[TEST] asset=BTC");
    println!("[TEST] side=BUY");
    println!("[TEST] qty={}", quantity);
    println!("[TEST] current_mid={}", current_mid);
    println!("[TEST] limit_px={}", entry_limit_px);
    println!("[TEST] cloid={}", entry_cloid);


    // ============================================================
    // SUBMIT LIMIT ENTRY
    // ============================================================

    let entry_request =
        ClientOrderRequest {
            asset: "BTC".to_string(),

            is_buy: true,

            reduce_only: false,

            limit_px: entry_limit_px,

            sz: quantity,

            cloid: Some(
                entry_cloid
            ),

            order_type:
                ClientOrder::Limit(
                    ClientLimit {
                        tif: "Gtc".to_string(),
                    }
                ),
        };


    let entry_response =
        exchange_client
            .order(
                entry_request,
                None,
            )
            .await
            .expect(
                "Hyperliquid limit entry request failed"
            );


    println!();
    println!(
        "[TEST] LIMIT ENTRY RAW RESPONSE:\n{:#?}",
        entry_response,
    );


    // ============================================================
    // PARSE ENTRY RESPONSE
    // ============================================================

    let entry_response =
        match entry_response {
            ExchangeResponseStatus::Ok(response) => {
                response
            }

            ExchangeResponseStatus::Err(error) => {
                panic!(
                    "Hyperliquid rejected limit entry: {:?}",
                    error,
                );
            }
        };


    let entry_data =
        entry_response
            .data
            .expect(
                "Limit entry response contained no data"
            );


    let entry_status =
        entry_data
            .statuses
            .first()
            .cloned()
            .expect(
                "Limit entry response contained no status"
            );


    match entry_status {
        ExchangeDataStatus::Filled(order) => {
            println!(
                "[TEST] LIMIT ENTRY IMMEDIATE FILLED | {:#?}",
                order,
            );
        }

        ExchangeDataStatus::Resting(order) => {
            println!(
                "[TEST] LIMIT ENTRY RESTING | {:#?}",
                order,
            );

            panic!(
                "Entry unexpectedly rested. Increase crossing tolerance if current market moved too quickly."
            );
        }

        ExchangeDataStatus::Error(error) => {
            panic!(
                "Hyperliquid limit entry error: {:?}",
                error,
            );
        }

        other => {
            panic!(
                "Unexpected limit entry status: {:?}",
                other,
            );
        }
    }


    // ============================================================
    // WAIT FOR ENTRY WS EVENTS
    // ============================================================

    // The REST response says the order immediately filled, but we deliberately
    // leave both execution streams running before sending the exit.
    //
    // This gives OrderUpdates and UserEvents time to show the exact lifecycle
    // that the production mapper must handle.
    println!();
    println!(
        "[TEST] waiting 5 seconds for entry WS events..."
    );


    sleep(
        Duration::from_secs(5)
    )
    .await;


    // ============================================================
    // GET FRESH MID FOR EXIT
    // ============================================================

    let exit_mid =
        (*rx_mid.borrow())
            .expect(
                "No BTC mid available before exit"
            );


    // ============================================================
    // EXIT PARAMETERS
    // ============================================================

    // A SELL limit becomes marketable when its limit is sufficiently below
    // the current bid.
    //
    // Again we cross the observed mid by 0.50%.
    let exit_limit_px =
        (exit_mid * 0.995)
            .floor();


    let exit_cloid =
        Uuid::new_v4();


    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT MARKETABLE REDUCE-ONLY LIMIT EXIT");
    println!("============================================================");
    println!("[TEST] asset=BTC");
    println!("[TEST] side=SELL");
    println!("[TEST] qty={}", quantity);
    println!("[TEST] current_mid={}", exit_mid);
    println!("[TEST] limit_px={}", exit_limit_px);
    println!("[TEST] reduce_only=true");
    println!("[TEST] cloid={}", exit_cloid);


    // ============================================================
    // SUBMIT LIMIT EXIT
    // ============================================================

    let exit_request =
        ClientOrderRequest {
            asset: "BTC".to_string(),

            is_buy: false,

            // CRITICAL:
            //
            // This is an EXIT order. It must never accidentally create a
            // short position if our assumed local position disagrees with
            // Hyperliquid.
            reduce_only: true,

            limit_px: exit_limit_px,

            sz: quantity,

            cloid: Some(
                exit_cloid
            ),

            order_type:
                ClientOrder::Limit(
                    ClientLimit {
                        tif: "Gtc".to_string(),
                    }
                ),
        };


    let exit_response =
        exchange_client
            .order(
                exit_request,
                None,
            )
            .await
            .expect(
                "Hyperliquid limit exit request failed"
            );


    println!();
    println!(
        "[TEST] LIMIT EXIT RAW RESPONSE:\n{:#?}",
        exit_response,
    );


    // ============================================================
    // PARSE EXIT RESPONSE
    // ============================================================

    let exit_response =
        match exit_response {
            ExchangeResponseStatus::Ok(response) => {
                response
            }

            ExchangeResponseStatus::Err(error) => {
                panic!(
                    "Hyperliquid rejected limit exit: {:?}",
                    error,
                );
            }
        };


    let exit_data =
        exit_response
            .data
            .expect(
                "Limit exit response contained no data"
            );


    let exit_status =
        exit_data
            .statuses
            .first()
            .cloned()
            .expect(
                "Limit exit response contained no status"
            );


    match exit_status {
        ExchangeDataStatus::Filled(order) => {
            println!(
                "[TEST] LIMIT EXIT IMMEDIATE FILLED | {:#?}",
                order,
            );
        }

        ExchangeDataStatus::Resting(order) => {
            println!(
                "[TEST] LIMIT EXIT RESTING | {:#?}",
                order,
            );

            panic!(
                "Exit unexpectedly rested. MANUALLY CHECK THE BTC POSITION before rerunning."
            );
        }

        ExchangeDataStatus::Error(error) => {
            panic!(
                "Hyperliquid limit exit error: {:?}",
                error,
            );
        }

        other => {
            panic!(
                "Unexpected limit exit status: {:?}",
                other,
            );
        }
    }


    // ============================================================
    // WAIT FOR FINAL WS EVENTS
    // ============================================================

    println!();
    println!(
        "[TEST] waiting 10 seconds for final WS events..."
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


    mids_info_client
        .unsubscribe(
            mids_subscription_id
        )
        .await
        .expect(
            "Failed to unsubscribe AllMids"
        );


    order_task.abort();
    user_task.abort();
    mids_task.abort();


    // ============================================================
    // DIAGNOSTIC ASSERTIONS
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
        "[TEST] Hyperliquid filled-limit-order WebSocket diagnostic completed."
    );
}