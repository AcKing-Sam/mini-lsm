#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        Ok(Self {
            is_valid: iter.is_valid(),
            inner: iter,
            upper: upper,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.upper.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().raw_ref() < key.as_ref(),
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }
        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            let res = self.iter.next();
            if res.is_err() {
                self.has_errored = true;
                return res;
            }
        }
        Ok(())
    }
}
