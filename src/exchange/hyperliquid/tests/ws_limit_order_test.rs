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
    ClientCancelRequestCloid,
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
    sync::mpsc::unbounded_channel,
    time::{
        sleep,
        Duration,
    },
};

use uuid::Uuid;


// ============================================================
// HYPERLIQUID LIMIT ORDER + CANCEL + EXECUTION WS DIAGNOSTIC TEST
// ============================================================
//
// Run:
//
// cargo test test_hyperliquid_limit_order_cancel_ws_updates -- --nocapture
//
// WARNING:
//
// This test uses Hyperliquid MAINNET and places a REAL limit order.
//
// The limit price is intentionally placed BELOW the current BTC market so
// the BUY order should rest instead of immediately executing.
//
// IMPORTANT:
//
// A BUY limit executes at the limit price OR BETTER.
//
// Therefore:
//
//     BTC market ~= $77,200
//
//     BUY @ $79,000
//         -> marketable
//         -> likely fills immediately
//
//     BUY @ $75,000
//         -> below market
//         -> should remain resting
//
// This test is specifically trying to observe:
//
//     Submit
//        ↓
//     Resting / Working
//        ↓
//     wait 10 seconds
//        ↓
//     Cancel by CLOID
//        ↓
//     Cancelled
//
// through BOTH:
//
//     Subscription::OrderUpdates
//     Subscription::UserEvents
//
// This gives us the raw venue behavior needed to implement the canonical
// Engine limit-order lifecycle:
//
//     PendingSubmit
//         ↓
//     Working
//         ↓
//     PendingCancel
//         ↓
//     Cancelled
//
// If the order unexpectedly fills before cancellation, the test will NOT
// attempt to cancel it and will report that condition instead.


#[tokio::test]
async fn test_hyperliquid_limit_order_cancel_ws_updates() {
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
    println!("[TEST] HYPERLIQUID LIMIT ORDER + CANCEL + WS");
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


    // Give both subscriptions time to become active before submitting.
    sleep(
        Duration::from_secs(2)
    )
    .await;


    // ============================================================
    // LIMIT ORDER PARAMETERS
    // ============================================================

    let asset =
        "BTC";

    let quantity =
        0.00019_f64;

    // IMPORTANT:
    //
    // BTC is currently around $77k for this diagnostic.
    //
    // A BUY limit ABOVE market would cross the spread and likely execute
    // immediately. We therefore place the BUY below market so that we can
    // observe a real resting order and subsequently cancel it.
    let limit_px =
        75_000.0_f64;


    // ============================================================
    // CLOID
    // ============================================================

    // This is the client-generated correlation identity.
    //
    // We use the same CLOID to:
    //
    //     1. submit the limit order,
    //     2. identify it in WS messages,
    //     3. cancel it.
    //
    // This is especially useful for the production adapter because the
    // Engine knows the client identity before Hyperliquid assigns an OID.
    let cloid =
        Uuid::new_v4();


    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT LIMIT BUY");
    println!("============================================================");
    println!("[TEST] asset={}", asset);
    println!("[TEST] side=BUY");
    println!("[TEST] qty={}", quantity);
    println!("[TEST] limit_px={}", limit_px);
    println!("[TEST] cloid={}", cloid);


    // ============================================================
    // BUILD LIMIT ORDER
    // ============================================================

    let order_request =
        ClientOrderRequest {
            asset: asset.to_string(),

            is_buy: true,

            reduce_only: false,

            limit_px,

            sz: quantity,

            cloid: Some(
                cloid
            ),

            // GTC = Good-Til-Cancelled.
            //
            // Unlike IOC-style market execution, this order is allowed to
            // remain resting on the Hyperliquid book until:
            //
            //     - it fills,
            //     - we cancel it,
            //     - or the venue otherwise terminates it.
            order_type:
                ClientOrder::Limit(
                    ClientLimit {
                        tif: "Gtc".to_string(),
                    }
                ),
        };


    // ============================================================
    // SUBMIT LIMIT ORDER
    // ============================================================

    let order_response =
        exchange_client
            .order(
                order_request,
                None,
            )
            .await
            .expect(
                "Hyperliquid limit order request failed"
            );


    println!();
    println!(
        "[TEST] LIMIT ORDER RAW RESPONSE:\n{:#?}",
        order_response,
    );


    // ============================================================
    // PARSE SUBMISSION RESPONSE
    // ============================================================

    let order_response =
        match order_response {
            ExchangeResponseStatus::Ok(response) => {
                response
            }

            ExchangeResponseStatus::Err(error) => {
                panic!(
                    "Hyperliquid rejected limit order: {:?}",
                    error,
                );
            }
        };


    let order_data =
        order_response
            .data
            .expect(
                "Limit order response contained no data"
            );


    let order_status =
        order_data
            .statuses
            .first()
            .cloned()
            .expect(
                "Limit order response contained no status"
            );


    // ============================================================
    // DETERMINE WHETHER ORDER IS RESTING
    // ============================================================

    let should_cancel =
        match order_status {
            ExchangeDataStatus::Resting(order) => {
                println!();
                println!(
                    "[TEST] LIMIT ORDER RESTING | {:#?}",
                    order,
                );

                println!(
                    "[TEST] venue_order_id={}",
                    order.oid,
                );

                true
            }


            ExchangeDataStatus::Filled(order) => {
                println!();
                println!(
                    "[TEST] LIMIT ORDER FILLED BEFORE CANCEL | {:#?}",
                    order,
                );

                println!(
                    "[TEST] order cannot be cancelled because it already filled"
                );

                false
            }


            ExchangeDataStatus::Error(error) => {
                panic!(
                    "Hyperliquid limit order returned error: {:?}",
                    error,
                );
            }


            other => {
                panic!(
                    "Unexpected limit order status: {:?}",
                    other,
                );
            }
        };


    // ============================================================
    // WAIT WHILE ORDER RESTS
    // ============================================================

    if should_cancel {
        println!();
        println!(
            "[TEST] waiting 10 seconds while limit order rests..."
        );


        sleep(
            Duration::from_secs(10)
        )
        .await;


        // ========================================================
        // CANCEL BY CLOID
        // ========================================================

        println!();
        println!("============================================================");
        println!("[TEST] CANCEL LIMIT ORDER");
        println!("============================================================");
        println!("[TEST] asset={}", asset);
        println!("[TEST] cloid={}", cloid);


        let cancel_request =
            ClientCancelRequestCloid {
                asset: asset.to_string(),

                cloid,
            };


        let cancel_response =
            exchange_client
                .cancel_by_cloid(
                    cancel_request,
                    None,
                )
                .await
                .expect(
                    "Hyperliquid cancel_by_cloid request failed"
                );


        println!();
        println!(
            "[TEST] CANCEL RAW RESPONSE:\n{:#?}",
            cancel_response,
        );


        // ========================================================
        // PARSE CANCEL RESPONSE
        // ========================================================

        match cancel_response {
            ExchangeResponseStatus::Ok(response) => {
                println!(
                    "[TEST] CANCEL ACCEPTED | {:#?}",
                    response,
                );
            }

            ExchangeResponseStatus::Err(error) => {
                panic!(
                    "Hyperliquid rejected limit order cancellation: {:?}",
                    error,
                );
            }
        }
    }


    // ============================================================
    // WAIT FOR FINAL WS EVENTS
    // ============================================================

    // Cancellation acknowledgement through REST and cancellation observation
    // through OrderUpdates are separate asynchronous facts.
    //
    // Keep both subscriptions alive so we can see exactly what Hyperliquid
    // sends after cancel_by_cloid().
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


    order_task.abort();
    user_task.abort();


    // ============================================================
    // BASIC DIAGNOSTIC ASSERTION
    // ============================================================

    // OrderUpdates is the important stream for this particular test because
    // a resting/cancelled order may never produce a fill UserEvent.
    //
    // Therefore we intentionally DO NOT assert:
    //
    //     user_events_seen > 0
    //
    // No execution occurred if the order simply rested and was cancelled.
    assert!(
        order_updates_seen > 0,
        "No Hyperliquid OrderUpdates messages were observed"
    );


    println!();
    println!(
        "[TEST] Hyperliquid limit-order cancellation WebSocket diagnostic completed."
    );
}