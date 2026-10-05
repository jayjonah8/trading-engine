#![allow(dead_code)]

use serde::Deserialize;


// ============================================================
// HYPERLIQUID HISTORICAL ARCHIVE TYPES
// ============================================================

/// One block record from Hyperliquid's `node_fills_by_block` archive.
///
/// Each line in the decompressed LZ4 file contains one JSON object.
#[derive(Debug, Deserialize)]
pub struct HyperliquidFillBlock {
    pub local_time: String,
    pub block_time: String,
    pub block_number: u64,
    pub events: Vec<(String, HyperliquidHistoricalFill)>,
}

/// One account-side fill contained in a historical block.
///
/// A single market execution normally appears once for each participant.
/// `tid` identifies the underlying trade and is used to deduplicate those
/// participant fills into one shared `HistoricalTrade`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidHistoricalFill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub time: i64,
    pub tid: u64,
    pub crossed: bool,
}