#![allow(dead_code)]

use anyhow::Result;

use hyperliquid_rust_sdk::Meta;


// ============================================================
// FETCH HYPERLIQUID META
// ============================================================

/// Fetches the full Hyperliquid Meta universe.
///
/// The API returns:
///
///     (Meta, AssetContexts)
///
/// We only need Meta here.
///
/// Meta is fetched once at startup and then periodically refreshed by the
/// lightweight metadata poller.
pub async fn fetch_meta() -> Result<Meta> {
    #[derive(serde::Serialize)]
    struct ReqBody<'a> {
        r#type: &'a str,
    }

    #[derive(serde::Deserialize)]
    struct MetaAndCtxs(
        Meta,
        serde_json::Value,
    );


    let body =
        ReqBody {
            r#type: "metaAndAssetCtxs",
        };


    let MetaAndCtxs(
        meta,
        _,
    ) =
        reqwest::Client::new()
            .post(
                "https://api.hyperliquid.xyz/info"
            )
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json",
            )
            .json(
                &body
            )
            .send()
            .await?
            .error_for_status()?
            .json::<MetaAndCtxs>()
            .await?;


    Ok(
        meta
    )
}