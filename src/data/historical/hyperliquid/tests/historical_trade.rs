#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use lz4_flex::frame::FrameDecoder;

use crate::data::historical::{
    hyperliquid::types::HyperliquidFillBlock,
    types::HistoricalTrade,
};

use crate::data::historical::hyperliquid::fetch::fetch_hyperliquid_historical_trades;

// test_hyperliquid_historical_trades
//     → low-level archive/parser + dedup invariant test

// test_prod_hyperliquid_historical_trades
//     → actual fetch_hyperliquid_historical_trades() API test


// ============================================================
// PRODUCTION HYPERLIQUID HISTORICAL TRADES
// ============================================================
// Tests the production Hyperliquid historical trade fetcher using a
// locally cached hourly `node_fills_by_block` archive.
//
// The fetcher reads the LZ4 archive, filters requested symbols and time,
// deduplicates participant fills by trade ID, and returns the shared
// `HistoricalTrade` format used by the backtest system.
//
// Run:
// cargo test test_prod_hyperliquid_historical_trades -- --nocapture

#[test]
fn test_prod_hyperliquid_historical_trades() {
    let trades = fetch_hyperliquid_historical_trades(
        &["BTC", "ETH", "SOL", "HYPE", "NEAR", "ZEC"],
        "2025-07-27T10:00:00Z",
        "2025-07-27T11:00:00Z",
    )
    .expect("Failed to get Hyperliquid historical trades");

    println!("Received {} total trades", trades.len());

    for trade in trades.iter().take(100) {
        println!(
            "{} | {} | P: {:.6} | S: {}",
            trade.timestamp,
            trade.symbol,
            trade.price,
            trade.size,
        );
    }

    assert!(!trades.is_empty());
}



// ============================================================
// TEST HYPERLIQUID HISTORICAL TRADES
// ============================================================

// Tests parsing one locally cached hour from Hyperliquid's
// `node_fills_by_block` historical archive.
//
// The archive stores account-side fills, so the same underlying trade can
// appear once for each participant. `tid` is used to deduplicate those fills
// into one market-level `HistoricalTrade`.
//
// Run:
// cargo test test_hyperliquid_historical_trades -- --nocapture

#[test]
fn test_hyperliquid_historical_trades() {
    let home = std::env::var("HOME").expect("HOME environment variable is not set");

    let path = PathBuf::from(home)
        .join("trading-data/hyperliquid/node_fills_by_block/hourly/20250727/10.lz4");

    let file = File::open(&path).expect("Failed to open Hyperliquid historical archive");
    let decoder = FrameDecoder::new(file);
    let reader = BufReader::new(decoder);

    let requested_symbols: HashSet<&str> = ["BTC", "ETH", "SOL"].into_iter().collect();
    let mut trades_by_tid: HashMap<u64, HistoricalTrade> = HashMap::new();

    let mut block_count = 0usize;
    let mut matching_fill_count = 0usize;
    let mut duplicate_fill_count = 0usize;

    for line in reader.lines() {
        let line = line.expect("Failed to read decompressed Hyperliquid archive line");
        let block: HyperliquidFillBlock = serde_json::from_str(&line).expect("Failed to parse Hyperliquid fill block");

        block_count += 1;

        for (_, fill) in block.events {
            if !requested_symbols.contains(fill.coin.as_str()) {
                continue;
            }

            matching_fill_count += 1;

            let price = fill.px.parse::<f64>().expect("Invalid Hyperliquid fill price");
            let size = fill.sz.parse::<f64>().expect("Invalid Hyperliquid fill size");
            let timestamp = DateTime::<Utc>::from_timestamp_millis(fill.time).expect("Invalid Hyperliquid fill timestamp");

            let trade = HistoricalTrade {
                symbol: fill.coin,
                timestamp,
                price,
                size,
            };

            if let Some(existing) = trades_by_tid.get(&fill.tid) {
                duplicate_fill_count += 1;

                assert_eq!(existing.symbol, trade.symbol, "Duplicate tid has different symbol | tid={}", fill.tid);
                assert_eq!(existing.timestamp, trade.timestamp, "Duplicate tid has different timestamp | tid={}", fill.tid);
                assert_eq!(existing.price, trade.price, "Duplicate tid has different price | tid={}", fill.tid);
                assert_eq!(existing.size, trade.size, "Duplicate tid has different size | tid={}", fill.tid);

                continue;
            }

            trades_by_tid.insert(fill.tid, trade);
        }
    }

    let mut trades: Vec<HistoricalTrade> = trades_by_tid.into_values().collect();
    trades.sort_by_key(|trade| trade.timestamp);

    println!("\n========================================");
    println!("[HYPERLIQUID TEST] HISTORICAL TRADES");
    println!("========================================");
    println!("File: {}", path.display());
    println!("Blocks: {}", block_count);
    println!("Matching fills: {}", matching_fill_count);
    println!("Duplicate participant fills: {}", duplicate_fill_count);
    println!("Unique trades: {}", trades.len());
    println!("========================================");

    for trade in trades.iter().take(100) {
        println!("{} | {} | P: {:.6} | S: {}", trade.timestamp, trade.symbol, trade.price, trade.size);
    }

    println!("========================================");

    assert!(block_count > 0);
    assert!(!trades.is_empty());
    assert!(duplicate_fill_count > 0);
}