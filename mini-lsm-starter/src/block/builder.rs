#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use nom::character::complete::u16;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn split_u16_to_u8s(value: u16) -> (u8, u8) {
    let high_byte = (value >> 8) as u8; // Extract the higher 8 bits
    let low_byte = (value & 0xFF) as u8; // Extract the lower 8 bits
    (high_byte, low_byte)
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: Default::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // it's the first key addition
        if self.data.is_empty() {
            self.offsets.push(0);
            self.first_key = key.to_key_vec();

            let (high, low) = split_u16_to_u8s(key.len() as u16);
            let mut entry = vec![high, low];
            entry.extend_from_slice(key.raw_ref());

            let (high_v, low_v) = split_u16_to_u8s(value.len() as u16);
            entry.push(high_v);
            entry.push(low_v);

            entry.extend_from_slice(value);
            self.data.extend_from_slice(&entry);
            return true;
        }

        let data_length = self.data.len();
        // existing data + offset data + key + val + new kv offset size + new offset size
        if self.data.len() + data_length * 2 + key.len() + value.len() + 3 * 2 > self.block_size {
            return false;
        }
        let offset = self.offsets.last().unwrap() + key.len() as u16 + value.len() as u16 + 2 * 2;
        self.offsets.push(offset);

        let (high, low) = split_u16_to_u8s(key.len() as u16);
        let mut entry = vec![high, low];
        entry.extend_from_slice(key.raw_ref());

        let (high_v, low_v) = split_u16_to_u8s(value.len() as u16);
        entry.push(high_v);
        entry.push(low_v);
        entry.extend_from_slice(value);
        self.data.extend_from_slice(&entry);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
