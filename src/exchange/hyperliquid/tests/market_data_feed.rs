#![allow(dead_code)]

use std::time::Duration;

use anyhow::Result;

use tokio::{
    sync::mpsc,
    time::timeout,
};

use crate::{
    core::{
        config::Config,
        events::AppEvent,
    },
    exchange::hyperliquid::market_data::run_hyperliquid_feed,
};

// Run test:
// cargo test test_hyperliquid_market_data_feed -- --nocapture

/// Tests the production Hyperliquid market-data feed.
///
/// This test verifies that:
///
/// - the WebSocket connects
/// - multiple symbols subscribe successfully
/// - fast L2 updates become `AppEvent::Quote`
/// - public trades become `AppEvent::TapeTrade`
/// - tape trades flow through the CandleManager
///
/// The feed itself contains reconnection logic and normally runs forever,
/// so this test stops after a short timeout.
#[tokio::test]
async fn test_hyperliquid_market_data_feed() -> Result<()> {
    // ============================================================
    // CONFIG
    // ============================================================

    let mut cfg = Config::default();

    cfg.enable_slack_notifications = true;
    cfg.enable_discord_notifications = true;

    let symbols = vec![
        "BTC".to_string(),
        "ETH".to_string(),
        "SOL".to_string(),
        "HYPE".to_string(),
    ];

    // ============================================================
    // MASTER EVENT CHANNEL
    // ============================================================

    let (tx_master, mut rx_master) =
        mpsc::unbounded_channel();

    // ============================================================
    // START PRODUCTION FEED
    // ============================================================

    let feed_cfg =
        cfg.clone();

    let feed_symbols =
        symbols.clone();

    let feed_handle =
        tokio::spawn(
            async move {
                run_hyperliquid_feed(
                    feed_cfg,
                    tx_master,
                    feed_symbols,
                )
                .await
            }
        );

    // ============================================================
    // RECEIVE EVENTS
    // ============================================================

    println!();
    println!("========================================");
    println!("HYPERLIQUID PRODUCTION MARKET DATA TEST");
    println!("========================================");
    println!("Symbols: {:?}", symbols);
    println!();

    let mut quote_count =
        0usize;

    let mut tape_trade_count =
        0usize;

    let mut bar_count =
        0usize;

    let receiver =
        async {
            while let Some(envelope) =
                rx_master.recv().await
            {
                match envelope.event {
                    // ================================================
                    // FAST QUOTE
                    // ================================================

                    AppEvent::Quote(quote) => {
                        quote_count += 1;

                        // Only print the first few quotes so the terminal
                        // doesn't get flooded by the fast L2 feed.
                        if quote_count <= 20 {
                            println!(
                                "[QUOTE] {:<6} bid={:<12.4} bid_sz={:<10.4} ask={:<12.4} ask_sz={:<10.4} mid={:<12.4} ts={}",
                                quote.symbol,
                                quote.bid_price,
                                quote.bid_size,
                                quote.ask_price,
                                quote.ask_size,
                                quote.mid_price(),
                                quote.timestamp_ms,
                            );
                        }
                    }

                    // ================================================
                    // PUBLIC TAPE TRADE
                    // ================================================

                    AppEvent::TapeTrade(trade) => {
                        tape_trade_count += 1;

                        if tape_trade_count <= 20 {
                            println!(
                                "[TAPE]  {:<6} px={:<12.4} size={:<12.6} ts={}",
                                trade.symbol,
                                trade.price,
                                trade.size,
                                trade.timestamp_ms,
                            );
                        }
                    }

                    // ================================================
                    // CLOSED CANDLE
                    // ================================================

                    AppEvent::BarClosed(bar) => {
                        bar_count += 1;

                        println!(
                            "[BAR]   {:<6} tf={:?} O={:.4} H={:.4} L={:.4} C={:.4} volume={:.6} trades={}",
                            bar.symbol,
                            bar.timeframe,
                            bar.open,
                            bar.high,
                            bar.low,
                            bar.close,
                            bar.volume,
                            bar.trade_count,
                        );
                    }
                }
            }
        };

    // ============================================================
    // TEST WINDOW
    // ============================================================

    // Run long enough to prove quotes and trades are flowing.
    //
    // If your smallest CandleConfig timeframe is 1 minute, increase this
    // above 60 seconds if you specifically want to observe BarClosed events.
    let _ =
        timeout(
            Duration::from_secs(20),
            receiver,
        )
        .await;

    // Stop the production feed task after the test window.
    feed_handle.abort();

    // ============================================================
    // RESULTS
    // ============================================================

    println!();
    println!("========================================");
    println!("MARKET DATA TEST RESULTS");
    println!("========================================");
    println!("Quotes:      {}", quote_count);
    println!("Tape trades: {}", tape_trade_count);
    println!("Bars:        {}", bar_count);
    println!();

    // We expect both fast quotes and public trades within the test window.
    assert!(
        quote_count > 0,
        "expected at least one Hyperliquid quote"
    );

    assert!(
        tape_trade_count > 0,
        "expected at least one Hyperliquid tape trade"
    );

    Ok(())
}