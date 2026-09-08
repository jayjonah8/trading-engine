use crate::exchange::alpaca_utils::{
    get_alpaca_account_balance,
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
        get_alpaca_account_balance()
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