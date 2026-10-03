#![allow(dead_code)]

use ibapi::{
    contracts::Contract,
    register_timezone_alias,
    Client,
};

// Tests IBKR contract resolution and prints the complete ContractDetails
// response returned by the installed ibapi SDK.
//
// Creates a basic stock contract using IBKR SMART routing and USD, sends it
// to TWS, and requests the full matching contract details.
//
// The complete Debug output is intentionally printed so we can inspect every
// field currently exposed by the installed ibapi version before deciding which
// metadata should be mapped into the Engine's canonical InstrumentState.
//
// Run:
// cargo test test_ibkr_asset_info -- --nocapture
#[tokio::test]
async fn test_ibkr_asset_info() {
    let address = "127.0.0.1:7497";
    let symbol = "AAPL";
    let client_id = 103;

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    println!("\n========================================");
    println!("[IBKR TEST] CONTRACT RESOLUTION");
    println!("========================================");
    println!("Symbol:    {symbol}");
    println!("Address:   {address}");
    println!("Client ID: {client_id}");
    println!("========================================");

    let client = match Client::connect(address, client_id).await {
        Ok(client) => client,

        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            return;
        }
    };

    println!("\n[IBKR TEST] CONNECTED");

    // Creates a stock contract using SMART routing and USD.
    let contract = Contract::stock(symbol).build();

    println!("[IBKR TEST] REQUESTING CONTRACT DETAILS | symbol={symbol}");

    let details = match client.contract_details(&contract).await {
        Ok(details) => details,

        Err(error) => {
            println!("\n[IBKR CONTRACT DETAILS ERROR]");
            println!("{error:?}");

            client.disconnect().await;
            println!("[IBKR TEST] DISCONNECTED");

            return;
        }
    };

    assert!(!details.is_empty(), "IBKR returned no contract details for {symbol}");

    println!("[IBKR TEST] CONTRACTS RETURNED: {}", details.len());

    for (index, detail) in details.iter().enumerate() {
        // ============================================================
        // IMPORTANT CONTRACT FIELDS
        // ============================================================

        println!("\n========================================");
        println!("[IBKR CONTRACT {}]", index + 1);
        println!("========================================");
        println!("Symbol:           {}", detail.contract.symbol);
        println!("Contract ID:      {}", detail.contract.contract_id);
        println!("Security Type:    {}", detail.contract.security_type);
        println!("Exchange:         {}", detail.contract.exchange);
        println!("Primary Exchange: {}", detail.contract.primary_exchange);
        println!("Currency:         {}", detail.contract.currency);
        println!("Local Symbol:     {}", detail.contract.local_symbol);
        println!("Trading Class:    {}", detail.contract.trading_class);


        // ============================================================
        // COMPLETE CONTRACT DETAILS
        // ============================================================

        // Print the entire ContractDetails object exactly as exposed by the
        // installed ibapi SDK. This lets us discover available execution
        // metadata without guessing SDK field names.
        println!("\n[IBKR FULL CONTRACT DETAILS]");
        println!("{:#?}", detail);
        println!("========================================");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("========================================");
}

// Example Output:
//
// client ID: 103
// ========================================

// [IBKR TEST] CONNECTED
// [IBKR TEST] REQUESTING CONTRACT DETAILS | symbol=AAPL
// [IBKR TEST] CONTRACTS RETURNED: 1

// ========================================
// [IBKR CONTRACT 1]
// ========================================
// Symbol:           AAPL
// Contract ID:      265598
// Security Type:    STK
// Exchange:         SMART
// Primary Exchange: NASDAQ
// Currency:         USD
// Local Symbol:     AAPL
// Trading Class:    NMS

// [IBKR FULL CONTRACT DETAILS]
// ContractDetails {
//     contract: Contract {
//         contract_id: 265598,
//         symbol: Symbol(
//             "AAPL",
//         ),
//         security_type: Stock,
//         last_trade_date_or_contract_month: "",
//         strike: 0.0,
//         right: None,
//         multiplier: "",
//         exchange: Exchange(
//             "SMART",
//         ),
//         currency: Currency(
//             "USD",
//         ),
//         local_symbol: "AAPL",
//         primary_exchange: Exchange(
//             "NASDAQ",
//         ),
//         trading_class: "NMS",
//         include_expired: false,
//         security_id_type: None,
//         security_id: "",
//         combo_legs_description: "",
//         combo_legs: [],
//         delta_neutral_contract: None,
//         last_trade_date: None,
//         issuer_id: "",
//         description: "",
//     },
//     market_name: "NMS",
//     min_tick: 0.01,
//     price_magnifier: 1,
//     order_types: [
//         "ACTIVETIM",
//         "AD",
//         "ADDONT",
//         "ADJUST",
//         "ALERT",
//         "ALGO",
//         "ALLOC",
//         "AON",
//         "AVGCOST",
//         "BASKET",
//         "BENCHPX",
//         "CASHQTY",
//         "COND",
//         "CONDORDER",
//         "DARKONLY",
//         "DARKPOLL",
//         "DAY",
//         "DEACT",
//         "DEACTDIS",
//         "DEACTEOD",
//         "DIS",
//         "DUR",
//         "GAT",
//         "GTC",
//         "GTD",
//         "GTT",
//         "HID",
//         "IBKRATS",
//         "ICE",
//         "IMB",
//         "IOC",
//         "LIT",
//         "LMT",
//         "LOC",
//         "MIDPX",
//         "MIT",
//         "MKT",
//         "MOC",
//         "MTL",
//         "NGCOMB",
//         "NODARK",
//         "NONALGO",
//         "OCA",
//         "OPG",
//         "OPGREROUT",
//         "PEGBENCH",
//         "PEGMID",
//         "POSTATS",
//         "POSTONLY",
//         "PREOPGRTH",
//         "PRICECHK",
//         "REL",
//         "REL2MID",
//         "RELPCTOFS",
//         "RPI",
//         "RTH",
//         "SCALE",
//         "SCALEODD",
//         "SCALERST",
//         "SIZECHK",
//         "SNAPMID",
//         "SNAPMKT",
//         "SNAPREL",
//         "STP",
//         "STPLMT",
//         "SWEEP",
//         "TRAIL",
//         "TRAILLIT",
//         "TRAILLMT",
//         "TRAILMIT",
//         "WHATIF",
//     ],
//     valid_exchanges: [
//         "SMART",
//         "AMEX",
//         "NYSE",
//         "CBOE",
//         "PHLX",
//         "ISE",
//         "CHX",
//         "ARCA",
//         "NASDAQ",
//         "DRCTEDGE",
//         "BEX",
//         "BATS",
//         "EDGEA",
//         "BYX",
//         "IEX",
//         "EDGX",
//         "FOXRIVER",
//         "PEARL",
//         "NYSENAT",
//         "LTSE",
//         "MEMX",
//         "IBEOS",
//         "OVERNIGHT",
//         "TPLUS0",
//         "PSX",
//         "T24X",
//         "TXSE",
//     ],
//     under_contract_id: 0,
//     long_name: "APPLE INC",
//     contract_month: "",
//     industry: "Technology",
//     category: "Computers",
//     subcategory: "Computers",
//     time_zone_id: "US/Eastern",
//     trading_hours: [
//         "20261003:CLOSED",
//         "20261004:CLOSED",
//         "20261005:0400-20261005:2000",
//         "20261006:0400-20261006:2000",
//         "20261007:0400-20261007:2000",
//         "20261008:0400-20261008:2000",
//     ],
//     liquid_hours: [
//         "20261003:CLOSED",
//         "20261004:CLOSED",
//         "20261005:0930-20261005:1600",
//         "20261006:0930-20261006:1600",
//         "20261007:0930-20261007:1600",
//         "20261008:0930-20261008:1600",
//     ],
//     ev_rule: "",
//     ev_multiplier: 0.0,
//     agg_group: 1,
//     sec_id_list: [
//         TagValue {
//             tag: "ISIN",
//             value: "US0378331005",
//         },
//     ],
//     under_symbol: "",
//     under_security_type: "",
//     market_rule_ids: [
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//         "4563",
//     ],
//     real_expiration_date: "",
//     last_trade_time: "",
//     stock_type: "COMMON",
//     cusip: "",
//     ratings: "",
//     desc_append: "",
//     bond_type: "",
//     coupon_type: "",
//     callable: false,
//     putable: false,
//     coupon: 0.0,
//     convertible: false,
//     maturity: "",
//     issue_date: "",
//     next_option_date: "",
//     next_option_type: "",
//     next_option_partial: false,
//     notes: "",
//     min_size: Some(
//         0.0001,
//     ),
//     size_increment: Some(
//         0.0001,
//     ),
//     suggested_size_increment: Some(
//         40.0,
//     ),
//     fund_name: "",
//     fund_family: "",
//     fund_type: "",
//     fund_front_load: "",
//     fund_back_load: "",
//     fund_back_load_time_interval: "",
//     fund_management_fee: "",
//     fund_closed: false,
//     fund_closed_for_new_investors: false,
//     fund_closed_for_new_money: false,
//     fund_notify_amount: "",
//     fund_minimum_initial_purchase: "",
//     fund_subsequent_minimum_purchase: "",
//     fund_blue_sky_states: "",
//     fund_blue_sky_territories: "",
//     fund_distribution_policy_indicator: None,
//     fund_asset_type: None,
//     ineligibility_reasons: [],
// }
// ========================================

// [IBKR TEST] DISCONNECTED
// ========================================