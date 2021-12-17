//! This crate implements a hash table that is persisted on disc as a file and accessed via mmap.
//!
//! The hash table can store keys and values as `&[u8]` of arbitrary length.
//! With the `msgpack` feature enabled, any type that can be (de-)serialized with serde/msgpack can be stored.
//!
//! The hash table consists of two parts:
//! 1) an actual hash table that stores the hash of the key and the position of the data.
//! 2) a memory-managed data area where keys and values are stored.
//! Both parts grow and shrink automatically depending on usage.
//!
//! The used algorithms are optimized for performance so that the data storage should be faster that a regular
//! database.

use std::{fs::File, hash::Hasher, io, mem, path::Path};

use index::{Entry, EntryData, Hash, Index};
use memmngr::{MemoryManagment, Used};
use mmap::MMap;

mod index;
mod iter;
mod memmngr;
mod mmap;
#[cfg(feature = "msgpack")]
mod msgpack;
mod resize;
#[cfg(test)]
mod tests;

#[cfg(feature = "msgpack")]
pub use msgpack::TypedTable;
use siphasher::sip::SipHasher13;

const INDEX_HEADER: [u8; 16] = *b"rust-persist-01\n";

const MAX_USAGE: f64 = 0.9;
const MIN_USAGE: f64 = 0.35;
const INITIAL_INDEX_CAPACITY: usize = 128;
const INITIAL_DATA_SIZE: usize = 0;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WrongHeader,
}

#[repr(C)]
struct Header {
    header: [u8; 16],
    flags: [u8; 16],
    index_capacity: u32,
}

impl Header {
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.flags[0] & 1 == 1
    }

    #[inline]
    pub fn set_dirty(&mut self, dirty: bool) {
        self.flags[0] = self.flags[0] & 0xfe | if dirty { 1 } else { 0 }
    }
}

#[inline]
fn total_size(index_capacity: usize, data_size: u64) -> u64 {
    mem::size_of::<Header>() as u64 + index_capacity as u64 * mem::size_of::<Entry>() as u64 + data_size
}

#[inline]
fn hash_key(key: &[u8]) -> Hash {
    let mut hasher = SipHasher13::default();
    hasher.write(key);
    hasher.finish()
}

#[inline]
fn match_key(entry: &EntryData, data: &[u8], data_start: u64, key: &[u8]) -> bool {
    if key.is_empty() && entry.key_size == 0 {
        return true;
    }
    let start = (entry.position - data_start) as usize;
    let end = start + entry.key_size as usize;
    &data[start..end] == key
}

pub struct Table {
    fd: File,
    mmap: MMap,
    header: &'static mut Header,
    index: Index,
    max_entries: usize,
    min_entries: usize,
    data: &'static mut [u8],
    data_start: u64,
    zero_size_entries: usize,
    mem: MemoryManagment,
}

impl Table {
    fn new_index(path: &Path, create: bool) -> Result<Self, Error> {
        let opened_fd = mmap::open_fd(path, create)?;
        let mut mem = MemoryManagment::new(
            opened_fd.data_start as u64,
            opened_fd.data_start as u64 + opened_fd.data.len() as u64,
        );
        let mut count = 0;
        let mut zero_size_entries = 0;
        for entry in opened_fd.index_entries.iter_mut() {
            if entry.is_used() {
                if create {
                    entry.clear()
                } else {
                    if entry.data.size > 0 {
                        mem.set_used(entry.data.position, entry.data.size, entry.hash);
                    } else {
                        zero_size_entries += 1;
                    }
                    count += 1;
                }
            }
        }
        mem.fix_up();
        let mut index = Index::new(opened_fd.index_entries, count);
        if opened_fd.header.is_dirty() {
            index.reinsert_all();
            assert!(index.is_valid(), "Inconsistent after reinsert");
            opened_fd.header.set_dirty(false);
        }
        let tbl = Self {
            max_entries: (opened_fd.header.index_capacity as f64 * MAX_USAGE) as usize,
            min_entries: (opened_fd.header.index_capacity as f64 * MIN_USAGE) as usize,
            fd: opened_fd.fd,
            mmap: opened_fd.mmap,
            index,
            mem,
            zero_size_entries,
            header: opened_fd.header,
            data: opened_fd.data,
            data_start: opened_fd.data_start as u64,
        };
        debug_assert!(tbl.is_valid(), "Inconsistent after creation");
        Ok(tbl)
    }

    #[inline]
    /// Open an existing database from the given path.
    ///
    /// Warning: Concurrent uses of the same database will result in data loss and other weird/unsafe behaviour.
    /// Make sure to only open the database once.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), false)
    }

    #[inline]
    /// Creates a new empty database. If the file exists, it will be overwritten.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), true)
    }

    pub(crate) fn allocate_data(&mut self, hash: Hash, size: u32) -> Result<u64, Error> {
        if size == 0 {
            self.zero_size_entries += 1;
            return Ok(0)
        }
        match self.mem.allocate(size, hash) {
            Some(pos) => Ok(pos),
            None => {
                self.extend_data(size)?;
                Ok(self.mem.allocate(size, hash).expect("Still not enough space after extend"))
            }
        }
    }

    pub(crate) fn free_data(&mut self, pos: u64) -> bool {
        if pos == 0 {
            self.zero_size_entries -= 1;
            true
        } else {
            self.mem.free(pos)
        }
    }

    #[inline]
    pub(crate) fn get_data(&self, pos: u64, len: u32) -> &[u8] {
        if len == 0 {
            return &[]
        }
        debug_assert!(pos >= self.data_start);
        debug_assert!(pos + len as u64 <= self.data_start + self.data.len() as u64);
        &self.data[(pos - self.data_start) as usize..(pos + len as u64 - self.data_start) as usize]
    }

    #[inline]
    pub(crate) fn get_data_mut(&mut self, pos: u64, len: u32) -> &mut [u8] {
        if len == 0 {
            return &mut []
        }
        debug_assert!(pos >= self.data_start);
        debug_assert!(pos + len as u64 <= self.data_start + self.data.len() as u64);
        &mut self.data[(pos - self.data_start) as usize..(pos + len as u64 - self.data_start) as usize]
    }

    #[inline]
    /// Returns the number of key/value pairs stored in the database.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    /// Returns the raw size of the database in bytes.
    pub fn size(&self) -> u64 {
        self.mmap.len() as u64
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.index.len() == 0
    }

    #[inline]
    pub fn flush(&self) -> Result<(), Error> {
        self.mmap.flush().map_err(Error::Io)
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let hash = hash_key(key);
        if let Some(entry) = self.index.index_get(hash, |e| match_key(e, self.data, self.data_start, key)) {
            Some(self.get_data(entry.position + entry.key_size as u64, entry.size - entry.key_size as u32))
        } else {
            None
        }
    }

    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut [u8]> {
        let hash = hash_key(key);
        if let Some(entry) = self.index.index_get(hash, |e| match_key(e, self.data, self.data_start, key)) {
            Some(self.get_data_mut(entry.position + entry.key_size as u64, entry.size - entry.key_size as u32))
        } else {
            None
        }
    }

    pub fn set(&mut self, key: &[u8], data: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.maybe_extend_index()?;
        self.maybe_shrink_data()?;
        let hash = hash_key(key);
        let len = (key.len() + data.len()) as u32;
        let pos = self.allocate_data(hash, len)?;
        if len > 0 {
            let space = self.get_data_mut(pos, len);
            space[..key.len()].copy_from_slice(key);
            space[key.len()..].copy_from_slice(data);
        }
        let entry = EntryData { position: pos, size: len, key_size: key.len() as u16, flags: 0 };
        let result = {
            let data = &self.data;
            let data_start = self.data_start;
            self.index.index_set(hash, |e| match_key(e, data, data_start, key), entry)
        };
        if let Some(old) = result {
            self.free_data(old.position);
            let old_data = self.get_data_mut(old.position + old.key_size as u64, old.size - old.key_size as u32);
            Ok(Some(old_data))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.maybe_shrink_index()?;
        self.maybe_shrink_data()?;
        let hash = hash_key(key);
        let result = {
            let data = &self.data;
            let data_start = self.data_start;
            self.index.index_delete(hash, |e| match_key(e, data, data_start, key))
        };
        if let Some(old) = result {
            self.free_data(old.position);
            let old_data = self.get_data_mut(old.position + old.key_size as u64, old.size - old.key_size as u32);
            Ok(Some(old_data))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn clear(&mut self) -> Result<(), Error> {
        self.resize_fd(INITIAL_INDEX_CAPACITY, INITIAL_DATA_SIZE as u64)?;
        self.index.clear();
        self.mem.clear();
        self.zero_size_entries = 0;
        self.header.index_capacity = INITIAL_INDEX_CAPACITY as u32;
        Ok(())
    }

    #[inline]
    pub fn close(self) {
        // nothing to do, just drop self
    }

    pub fn is_valid(&self) -> bool {
        let mut valid = true;
        valid &= self.index.is_valid();
        valid &= self.mem.is_valid();
        if self.mem.start() < self.data_start {
            println!("Data begins before data start: {} vs {}", self.mem.start(), self.data_start);
            valid = false;
        }
        if self.mem.end() > self.data_start + self.data.len() as u64 {
            println!("Data ends after data end: {} vs {}", self.mem.end(), self.data_start + self.data.len() as u64);
            valid = false;
        }
        let used = self.mem.get_used();
        for entry in self.index.get_entries() {
            if entry.is_used() && entry.data.size > 0
                && !used.contains(&Used { start: entry.data.position, size: entry.data.size, hash: entry.hash })
            {
                println!("Index entry at {} does not exist in mem", entry.data.position);
                valid = false;
            }
        }
        if used.len() + self.zero_size_entries != self.index.len() {
            println!("Index and data disagree about entry count: {} vs {}", self.index.len(), used.len());
            valid = false;
        }
        valid
    }
}
