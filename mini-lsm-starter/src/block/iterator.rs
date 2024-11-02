#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let block_ref = Arc::clone(&block);
        let mut iterator = Self::new(block_ref);
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let block_ref = Arc::clone(&block);
        let mut iterator = Self::new(block_ref);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let mut start_idx = self.block.offsets[self.idx];
        let kl_high = self.block.data[start_idx as usize];
        start_idx += 1;
        let kl_low = self.block.data[start_idx as usize];
        start_idx += 1;
        let kl = ((kl_high as u16) << 8) | (kl_low as u16);
        let mut end_idx = start_idx + kl;
        let key = &self.block.data[start_idx as usize..end_idx as usize];
        start_idx += kl;

        let vl_high = self.block.data[start_idx as usize];
        start_idx += 1;
        let vl_low = self.block.data[start_idx as usize];
        start_idx += 1;
        let vl = ((vl_high as u16) << 8) | (vl_low as u16);
        end_idx = start_idx + vl;
        let mut val = &self.block.data[start_idx as usize..end_idx as usize];
        val
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        if self.idx + 1 == self.block.offsets.len() {
            return false;
        }
        true
    }

    fn extract_key(&self, start_idx: usize) -> KeyVec {
        let mut start = start_idx as u16;
        let kl_high: u8 = self.block.data[start as usize];
        start += 1;
        let kl_low: u8 = self.block.data[start as usize];
        start += 1;
        let kl = ((kl_high as u16) << 8) | kl_low as u16;

        let end_idx = start + kl;
        let key = self.block.data[start as usize..end_idx as usize].to_vec();
        KeyVec::from_vec(key)
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let idx = self.block.offsets[0];
        let key = self.extract_key(idx as usize);
        self.first_key = key.clone();
        self.key = key;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }
        self.idx += 1;
        self.key = self.extract_key(self.block.offsets[self.idx] as usize);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // we go from start to end
        self.seek_to_first();
        while self.is_valid() && self.key() < key {
            self.next();
        }
    }
}
