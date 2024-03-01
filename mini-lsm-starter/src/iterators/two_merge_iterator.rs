#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    flag: bool,
    is_valid: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        if !a.is_valid() && !b.is_valid() {
            Ok(TwoMergeIterator {
                a: a,
                b: b,
                flag: true,
                is_valid: false,
            })
        } else if !b.is_valid() {
            Ok(TwoMergeIterator {
                a: a,
                b: b,
                flag: true,
                is_valid: true,
            })
        } else if !a.is_valid() {
            Ok(TwoMergeIterator {
                a: a,
                b: b,
                flag: false,
                is_valid: true,
            })
        } else {
            if a.key() <= b.key() {
                Ok(TwoMergeIterator {
                    a: a,
                    b: b,
                    flag: true,
                    is_valid: true,
                })
            } else {
                Ok(TwoMergeIterator {
                    a: a,
                    b: b,
                    flag: false,
                    is_valid: true,
                })
            }
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;
    fn key(&self) -> Self::KeyType<'_> {
        if self.flag {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.flag {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid {
            return Ok(());
        }
        if self.flag {
            while self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?;
            }
            self.a.next()?;
            if !self.a.is_valid() {
                if !self.b.is_valid() {
                    self.is_valid = false;
                    Ok(())
                } else {
                    self.flag = false;
                    Ok(())
                }
            } else {
                if self.b.is_valid() && self.a.key() > self.b.key() {
                    self.flag = false;
                }
                Ok(())
            }
        } else {
            while self.a.is_valid() && self.a.key() == self.b.key() {
                self.a.next()?;
            }
            self.b.next()?;
            if !self.b.is_valid() {
                if !self.a.is_valid() {
                    self.is_valid = false;
                    Ok(())
                } else {
                    self.flag = false;
                    Ok(())
                }
            } else {
                if self.a.is_valid() && self.a.key() <= self.b.key() {
                    self.flag = true;
                }
                Ok(())
            }
        }
    }
}
