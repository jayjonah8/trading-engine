#![allow(dead_code)]

use serde::Deserialize;


// ============================================================
// HYPERLIQUID MARKET ASSET
// ============================================================

#[derive(Clone, Debug)]
pub struct MarketAsset {
    pub symbol: String,
    pub volume: f64,
    pub max_leverage: u32,
    pub pnl_24h_pct: f64,
    pub open_interest: f64,
    pub funding: f64,
    pub impact_pxs: (f64, f64),
}


// ============================================================
// HYPERLIQUID META + ASSET CONTEXT
// ============================================================

#[derive(Debug, Deserialize)]
pub struct Meta {
    pub universe: Vec<UniverseItem>,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct UniverseItem {
    pub name: String,
    #[serde(default)] pub maxLeverage: Option<f64>,
    #[serde(default)] pub isDelisted: Option<bool>,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct AssetCtx {
    #[serde(default)] pub dayNtlVlm: Option<String>,
    #[serde(default)] pub markPx: Option<String>,
    #[serde(default)] pub midPx: Option<String>,
    #[serde(default)] pub oraclePx: Option<String>,
    #[serde(default)] pub prevDayPx: Option<String>,
    #[serde(default)] pub openInterest: Option<String>,
    #[serde(default)] pub funding: Option<String>,
    #[serde(default)] pub impactPxs: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct MetaAndCtxs(pub Meta, pub Vec<AssetCtx>);