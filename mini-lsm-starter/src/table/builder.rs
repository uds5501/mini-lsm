#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::mem;
use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::{BufMut, Bytes};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    fn replace_block_builder(&mut self) {
        let new_builder = BlockBuilder::new(self.block_size);
        let old_builder = mem::replace(&mut self.builder, new_builder);
        let block = old_builder.build();
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(mem::take(&mut self.first_key))),
            last_key: KeyBytes::from_bytes(Bytes::from(mem::take(&mut self.last_key))),
        };
        self.meta.push(meta);
        self.data.extend(block.encode());
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let inserted = self.builder.add(key, value);
        if !inserted {
            self.replace_block_builder();
            self.add(key, value);
        }

        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }
        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.replace_block_builder();
        let sst_first_key = self.meta.first().unwrap().first_key.clone().into_inner();
        let sst_last_key = self.meta.last().unwrap().last_key.clone().into_inner();
        let mut final_data = self.data;
        let meta_offset = final_data.len();

        // now encode and add metadata
        BlockMeta::encode_block_meta(&self.meta, final_data.as_mut());
        final_data.put_u32(meta_offset as u32);

        let res = FileObject::create(path.as_ref(), final_data)?;
        let ss_table = SsTable {
            file: res,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(sst_first_key),
            last_key: KeyBytes::from_bytes(sst_last_key),
            bloom: None,
            max_ts: 0,
        };
        Ok(ss_table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
