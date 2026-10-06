#![allow(dead_code)]


use crate::exchange::alpaca::api::balance::{
    get_alpaca_account_balances,
};


// ============================================================
// TEST ALPACA ACCOUNT BALANCE
// ============================================================

// cargo test test_alpaca_account_balance -- --nocapture

#[tokio::test]
async fn test_alpaca_account_balance() {
    dotenvy::dotenv().ok();


    // ============================================================
    // GET ACCOUNT BALANCE
    // ============================================================

    let account_balance =
        get_alpaca_account_balances()
            .await
            .expect(
                "Failed to get Alpaca account balance"
            );


    // ============================================================
    // PRINT ACCOUNT BALANCE STRUCT
    // ============================================================

    println!();
    println!("# ========================================");
    println!("ALPACA ACCOUNT BALANCE");
    println!();

    println!(
        "{:#?}",
        account_balance
    );
}