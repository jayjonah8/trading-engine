#![allow(dead_code)]

use std::{
    sync::Arc,
    time::Duration,
};

use hyperliquid_rust_sdk::Meta;

use tokio::{
    sync::{
        mpsc::UnboundedSender,
        RwLock,
    },
    time,
};

use crate::{
    core::{
        execution::Venue,
        venue_state::VenueStateEvent,
    },

    exchange::hyperliquid::meta::{
        fetch_meta,
        map_meta_to_instruments,
    },
};


// ============================================================
// HYPERLIQUID META POLLER
// ============================================================

/// Periodically refreshes Hyperliquid instrument metadata.
///
/// The initial Meta snapshot is fetched during application startup.
///
/// This poller only performs later refreshes and must never run during
/// historical backtesting.
///
/// Each successful refresh updates:
///
/// - the shared raw Hyperliquid Meta cache
/// - the canonical Engine instrument snapshot
pub async fn run_hyperliquid_meta_poller(
    meta_cache: Arc<RwLock<Meta>>,
    tx_venue_state: UnboundedSender<VenueStateEvent>,
) {
    let mut interval =
        time::interval(
            Duration::from_secs(
                2 * 60 * 60
            )
        );


    // Tokio's first interval tick is immediate.
    //
    // Startup already fetched Meta, so consume it and wait two hours
    // before performing the first refresh.
    interval.tick().await;


    loop {
        interval.tick().await;


        // ========================================================
        // FETCH META
        // ========================================================

        let meta =
            match fetch_meta().await {
                Ok(meta) => {
                    meta
                }

                Err(error) => {
                    eprintln!(
                        "[HYPERLIQUID META POLLER] FETCH FAILED | error={:?}",
                        error,
                    );

                    // Keep the last valid raw and canonical snapshots.
                    continue;
                }
            };


        // ========================================================
        // MAP CANONICAL INSTRUMENT STATE
        // ========================================================

        let instruments =
            map_meta_to_instruments(
                &meta
            );


        let instrument_count =
            instruments.len();


        // ========================================================
        // UPDATE RAW META CACHE
        // ========================================================

        {
            let mut cached_meta =
                meta_cache
                    .write()
                    .await;

            *cached_meta =
                meta;
        }


        // ========================================================
        // UPDATE CANONICAL ENGINE SNAPSHOT
        // ========================================================

        if let Err(error) =
            tx_venue_state.send(
                VenueStateEvent::InstrumentsUpdated {
                    venue: Venue::Hyperliquid,
                    instruments,
                }
            )
        {
            eprintln!(
                "[HYPERLIQUID META POLLER] VENUE STATE CHANNEL CLOSED | error={}",
                error,
            );

            return;
        }


        println!(
            "[HYPERLIQUID META POLLER] UPDATED | instruments={}",
            instrument_count,
        );
    }
}