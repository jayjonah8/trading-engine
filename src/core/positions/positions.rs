use std::collections::HashMap;

use crate::core::positions::{
    key::PositionKey,
    position::Position,
};

// TODO: One intentional choice: don’t remove a Position from the map when it becomes flat yet. 
// Keeping the flat record can be useful for reconciliation/accounting. We can decide on 
// pruning/archive behavior later.

/// Authoritative collection of positions known by the engine.
///
/// Positions are keyed by `PositionKey` rather than symbol alone so the same
/// instrument can be traded across multiple venues and accounts.
///
/// Position state should only be changed as the result of confirmed fills or
/// explicit venue reconciliation. Submitting an order does not itself create
/// or modify a position.
///
/// This container represents actual exposure, while `Trades` separately
/// represents strategy-level trade lifecycles.
#[derive(Debug, Default)]
pub struct Positions {
    positions: HashMap<PositionKey, Position>,
}

impl Positions {
    /// Creates an empty position collection.
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
        }
    }

    /// Returns a position by its venue/account/symbol key.
    pub fn get(&self, key: &PositionKey) -> Option<&Position> {
        self.positions.get(key)
    }

    /// Returns a mutable position by its venue/account/symbol key.
    ///
    /// Engine state handlers should be the primary users of mutable access.
    pub fn get_mut(&mut self, key: &PositionKey) -> Option<&mut Position> {
        self.positions.get_mut(key)
    }

    /// Returns the existing position or creates a new flat position.
    ///
    /// This is useful when processing the first fill for an instrument that
    /// does not yet have a position entry in engine state.
    pub fn get_or_create(
        &mut self,
        key: PositionKey,
        ts_ms: i64,
    ) -> &mut Position {
        self.positions
            .entry(key.clone())
            .or_insert_with(|| Position::new(key, ts_ms))
    }

    /// Inserts or replaces a position.
    ///
    /// Primarily useful for venue reconciliation where authoritative position
    /// state may be restored from the execution venue.
    pub fn insert(&mut self, position: Position) -> Option<Position> {
        self.positions.insert(position.key.clone(), position)
    }

    /// Returns the number of position records currently tracked.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Returns true when no positions are currently tracked.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Iterates over all tracked positions.
    pub fn iter(&self) -> impl Iterator<Item = (&PositionKey, &Position)> {
        self.positions.iter()
    }
}