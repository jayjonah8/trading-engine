#![allow(dead_code)]

use uuid::Uuid;

use crate::core::execution::ClientOrderId;


// ============================================================
// CLIENT ORDER ID <-> ALPACA CLIENT ORDER ID
// ============================================================

/// Converts the Engine's canonical `ClientOrderId` into the string
/// representation sent to Alpaca as `client_order_id`.
///
/// The canonical identity is UUID-backed, so the conversion is lossless.
///
/// Example:
///
///     ClientOrderId
///         ↓
///     7b805495-2c28-47fa-bd31-01acd2048e21
///         ↓
///     Alpaca client_order_id
pub fn to_alpaca_client_order_id(client_order_id: ClientOrderId) -> String {
    client_order_id.to_string()
}


/// Parses an Alpaca `client_order_id` back into the Engine's canonical
/// `ClientOrderId`.
///
/// Orders submitted by this Engine use UUID-form client order IDs, allowing
/// Alpaca WebSocket events to be correlated directly back to canonical
/// Engine orders.
///
/// Returns `None` if the venue value is not a valid UUID.
///
/// NOTE:
/// Alpaca may contain orders created outside this Engine whose client order
/// IDs are not UUIDs. Those orders intentionally fail canonical correlation
/// through this function.
pub fn client_order_id_from_alpaca(value: &str) -> Option<ClientOrderId> {
    let uuid = Uuid::parse_str(value).ok()?;
    Some(ClientOrderId::from_uuid(uuid))
}


// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpaca_client_order_id_round_trip() {
        let original = ClientOrderId::from_uuid(
            Uuid::parse_str("7b805495-2c28-47fa-bd31-01acd2048e21").unwrap()
        );

        let alpaca_value = to_alpaca_client_order_id(original);
        let parsed = client_order_id_from_alpaca(&alpaca_value).expect("failed to parse Alpaca client_order_id");

        assert_eq!(parsed, original);
    }

    #[test]
    fn test_invalid_alpaca_client_order_id() {
        let parsed = client_order_id_from_alpaca("not-a-valid-uuid");
        assert!(parsed.is_none());
    }
}