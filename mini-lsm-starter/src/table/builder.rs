#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{io::Read, path::Path};

use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

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
    pub fn new(_block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(_block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size: _block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.put(&key.raw_ref()[..]);
            self.last_key.put(&key.raw_ref()[..]);
        }
        if !self.builder.add(key, value) {
            // split a new block
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(self.first_key.clone().try_into().unwrap()),
                last_key: KeyBytes::from_bytes(self.last_key.clone().try_into().unwrap()),
            });
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            self.data.extend(builder.build().encode());
        } else {
            self.last_key.clear();
            self.last_key.put(&key.raw_ref()[..]);
            return;
        }
        assert!(self.builder.add(key, value));
        self.first_key.clear();
        self.last_key.clear();
        self.first_key.put(&key.raw_ref()[..]);
        self.last_key.put(&key.raw_ref()[..]);
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
        if !self.builder.is_empty() {
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(self.first_key.clone().try_into().unwrap()),
                last_key: KeyBytes::from_bytes(self.last_key.clone().try_into().unwrap()),
            });
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            self.data.extend(builder.build().encode());
        }
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        Ok(SsTable {
            file: FileObject::create(path.as_ref(), buf).unwrap(),
            block_meta_offset: meta_offset,
            id: id,
            block_cache: block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            max_ts: 0,
            bloom: None,
            block_meta: self.meta,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
