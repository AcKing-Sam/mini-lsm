#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
use super::Block;
use bytes::Buf;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
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
        let mut res = BlockIterator::new(block);
        res.seek_to_first();
        res
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut res = BlockIterator::new(block);
        res.seek_to_key(key);
        res
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        let entry_offsets_len = self.block.offsets.len();
        if self.idx < entry_offsets_len {
            let off = self.block.offsets[self.idx] as usize;
            let mut entry = &self.block.data[off..];
            let key_len = entry.get_u16() as usize;
            let key = &entry[..key_len];
            self.key.clear();
            self.key.append(key);
            entry.advance(key_len);
            let val_len = entry.get_u16() as usize;
            let value_offset_begin = off + key_len + SIZEOF_U16 * 2;
            let value_offset_end = value_offset_begin + val_len;
            self.value_range = (value_offset_begin, value_offset_end);
            entry.advance(val_len);
        } else {
            self.key.clear();
            self.value_range = (0, 0);
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let entry_offsets_len = self.block.offsets.len();
        self.idx += 1;
        if self.idx >= entry_offsets_len {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let off = self.block.offsets[self.idx] as usize;
        let mut entry = &self.block.data[off..];
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        self.key.append(key);
        entry.advance(key_len);
        let val_len = entry.get_u16() as usize;
        let value_offset_begin = off + key_len + SIZEOF_U16 * 2;
        let value_offset_end = value_offset_begin + val_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(val_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, _key: KeySlice) {
        let entry_offsets_len = self.block.offsets.len();
        self.idx = 0;
        while self.idx < entry_offsets_len {
            let off = self.block.offsets[self.idx] as usize;
            let mut entry = &self.block.data[off..];
            let key_len = entry.get_u16() as usize;
            let key = &entry[..key_len];
            self.key.clear();
            self.key.append(key);
            entry.advance(key_len);
            let val_len = entry.get_u16() as usize;
            let value_offset_begin = off + key_len + SIZEOF_U16 * 2;
            let value_offset_end = value_offset_begin + val_len;
            self.value_range = (value_offset_begin, value_offset_end);
            entry.advance(val_len);
            match self.key().cmp(&_key) {
                std::cmp::Ordering::Less => {
                    self.idx += 1;
                    continue;
                }
                std::cmp::Ordering::Greater => {
                    return;
                }
                std::cmp::Ordering::Equal => {
                    return;
                }
            }
        }
        self.key.clear();
        self.value_range = (0, 0);
    }
}
