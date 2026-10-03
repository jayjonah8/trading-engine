#![allow(dead_code)]

use crate::exchange::alpaca::api::asset::get_alpaca_asset;


// ============================================================
// TEST GET ALPACA ASSET INFO
// ============================================================

// cargo test test_get_alpaca_asset -- --ignored --nocapture

#[tokio::test]
#[ignore = "requires Alpaca API credentials"]
async fn test_get_alpaca_asset() {
    dotenvy::dotenv().ok();

    let symbol = "AAPL";

    let asset = get_alpaca_asset(symbol)
        .await
        .expect("failed to fetch Alpaca asset");

    println!("========================================");
    println!("ALPACA ASSET");
    println!("========================================");
    println!("ID:              {}", asset.id);
    println!("Symbol:          {}", asset.symbol);
    println!("Name:            {}", asset.name);
    println!("Class:           {}", asset.class);
    println!("Exchange:        {}", asset.exchange);
    println!("Status:          {}", asset.status);
    println!("Tradable:        {}", asset.tradable);
    println!("Marginable:      {}", asset.marginable);
    println!("Shortable:       {}", asset.shortable);
    println!("Easy To Borrow:  {}", asset.easy_to_borrow);
    println!("Fractionable:    {}", asset.fractionable);
    println!(
        "Maintenance MM:  {}",
        asset.maintenance_margin_requirement
    );
    println!("========================================");
}

// Example Output:
// ========================================
// ALPACA ASSET
// ========================================
// ID:              b0b6dd9d-8b9b-48a9-ba46-b9d54906e415
// Symbol:          AAPL
// Name:            Apple Inc. Common Stock
// Class:           us_equity
// Exchange:        NASDAQ
// Status:          active
// Tradable:        true
// Marginable:      true
// Shortable:       true
// Easy To Borrow:  true
// Fractionable:    true
// Maintenance MM:  30
// ========================================