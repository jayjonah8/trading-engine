use std::sync::Arc;

use hyperliquid_rust_sdk::Meta;

use tokio::sync::RwLock;

use crate::exchange::hyperliquid::meta::fetch_meta;


// ============================================================
// HYPERLIQUID TEST META
// ============================================================

/// Fetches Hyperliquid Meta and wraps it in the same shared cache used
/// by the production adapter.
///
/// Intended for Hyperliquid integration tests that already depend on the
/// live Hyperliquid API.
pub async fn fetch_test_meta_cache() -> Arc<RwLock<Meta>> {
    let meta =
        fetch_meta()
            .await
            .expect(
                "failed to fetch Hyperliquid Meta for integration test"
            );


    Arc::new(
        RwLock::new(
            meta
        )
    )
}