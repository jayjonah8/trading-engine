#![allow(dead_code)]

use uuid::Uuid;

use crate::core::execution::ClientOrderId;


// ============================================================
// CLIENT ORDER ID <-> HYPERLIQUID CLOID
// ============================================================

/// Converts the engine's canonical ClientOrderId into the UUID type expected
/// by Hyperliquid's order submission API.
///
/// Hyperliquid's SDK represents outbound CLOID as `Uuid`, while the engine
/// wraps the same UUID inside `ClientOrderId`.
///
/// This conversion is lossless.
pub fn to_hyperliquid_cloid(
    client_order_id: ClientOrderId,
) -> Uuid {
    client_order_id.as_uuid()
}


/// Converts a Hyperliquid UUID CLOID directly back into the engine's
/// canonical ClientOrderId.
///
/// This is useful anywhere the SDK exposes CLOID as `Uuid`.
pub const fn from_hyperliquid_cloid(
    cloid: Uuid,
) -> ClientOrderId {
    ClientOrderId::from_uuid(
        cloid
    )
}


/// Parses a CLOID returned by Hyperliquid WebSocket messages.
///
/// Hyperliquid returns CLOID through the execution streams in a compact
/// hexadecimal representation such as:
///
///     0xf8ad4c638f844518b41462960f7332b2
///
/// while the engine's canonical ClientOrderId displays as:
///
///     f8ad4c63-8f84-4518-b414-62960f7332b2
///
/// Both represent the exact same 128-bit UUID.
///
/// Parsing flow:
///
///     Hyperliquid WS CLOID string
///         ↓
///     remove optional "0x"
///         ↓
///     parse 32 hexadecimal characters as u128
///         ↓
///     reconstruct UUID
///         ↓
///     ClientOrderId
///
/// Returns `None` if the venue value is malformed.
pub fn client_order_id_from_ws_cloid(
    cloid: &str,
) -> Option<ClientOrderId> {
    let raw =
        cloid
            .strip_prefix("0x")
            .unwrap_or(cloid);

    if raw.len() != 32 {
        return None;
    }

    let value =
        u128::from_str_radix(
            raw,
            16,
        )
        .ok()?;

    let uuid =
        Uuid::from_u128(
            value
        );

    Some(
        ClientOrderId::from_uuid(
            uuid
        )
    )
}


// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyperliquid_ws_cloid_round_trip() {
        let original =
            ClientOrderId::from_uuid(
                Uuid::parse_str(
                    "f8ad4c63-8f84-4518-b414-62960f7332b2"
                )
                .unwrap()
            );

        let ws_value =
            "0xf8ad4c638f844518b41462960f7332b2";

        let parsed =
            client_order_id_from_ws_cloid(
                ws_value
            )
            .expect(
                "failed to parse Hyperliquid CLOID"
            );

        assert_eq!(
            parsed,
            original
        );
    }
}