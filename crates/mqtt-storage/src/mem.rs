//! # In-Memory Ring and Buffer Storage
//!
//! Provides compile-time bounded in-memory storage for offline queuing and session checkpoints.

use heapless::Vec;

/// Error type for bounded in-memory storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MemStorageError {
    KeyNotFound,
    CapacityFull,
    BufferTooSmall,
}

impl core::fmt::Display for MemStorageError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::KeyNotFound => write!(f, "Key not found in memory store"),
            Self::CapacityFull => write!(f, "Memory store capacity is full"),
            Self::BufferTooSmall => write!(f, "Provided read buffer is too small"),
        }
    }
}

/// Fixed-capacity static in-memory key-value store for embedded sessions.
#[derive(Debug, Clone)]
pub struct StaticMemStore<
    const MAX_ENTRIES: usize,
    const MAX_KEY_LEN: usize,
    const MAX_VAL_LEN: usize,
> {
    keys: Vec<Vec<u8, MAX_KEY_LEN>, MAX_ENTRIES>,
    values: Vec<Vec<u8, MAX_VAL_LEN>, MAX_ENTRIES>,
}

impl<const MAX_ENTRIES: usize, const MAX_KEY_LEN: usize, const MAX_VAL_LEN: usize> Default
    for StaticMemStore<MAX_ENTRIES, MAX_KEY_LEN, MAX_VAL_LEN>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const MAX_ENTRIES: usize, const MAX_KEY_LEN: usize, const MAX_VAL_LEN: usize>
    StaticMemStore<MAX_ENTRIES, MAX_KEY_LEN, MAX_VAL_LEN>
{
    /// Creates a new empty static memory store.
    pub const fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Stores a key-value pair.
    pub fn persist(&mut self, key: &[u8], value: &[u8]) -> Result<(), MemStorageError> {
        let mut k = Vec::new();
        let mut v = Vec::new();
        k.extend_from_slice(key)
            .map_err(|_| MemStorageError::BufferTooSmall)?;
        v.extend_from_slice(value)
            .map_err(|_| MemStorageError::BufferTooSmall)?;

        if let Some(idx) = self.keys.iter().position(|entry| entry.as_slice() == key) {
            self.values[idx] = v;
        } else {
            if self.keys.is_full() {
                return Err(MemStorageError::CapacityFull);
            }
            let _ = self.keys.push(k);
            let _ = self.values.push(v);
        }
        Ok(())
    }

    /// Retrieves the value associated with `key` into `out_buf`.
    pub fn load(&self, key: &[u8], out_buf: &mut [u8]) -> Result<Option<usize>, MemStorageError> {
        if let Some(idx) = self.keys.iter().position(|entry| entry.as_slice() == key) {
            let val = &self.values[idx];
            if out_buf.len() < val.len() {
                return Err(MemStorageError::BufferTooSmall);
            }
            out_buf[..val.len()].copy_from_slice(val.as_slice());
            Ok(Some(val.len()))
        } else {
            Ok(None)
        }
    }

    /// Removes a key from the store.
    pub fn remove(&mut self, key: &[u8]) -> Result<(), MemStorageError> {
        if let Some(idx) = self.keys.iter().position(|entry| entry.as_slice() == key) {
            self.keys.swap_remove(idx);
            self.values.swap_remove(idx);
            Ok(())
        } else {
            Err(MemStorageError::KeyNotFound)
        }
    }

    /// Clears the store.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.values.clear();
    }
}
