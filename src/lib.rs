#![warn(missing_docs)]
//! This crate implements a hash table that is persisted on disk as a file and accessed via mmap.
//!
//! The hash table can store keys and values as `&[u8]` of arbitrary length.
//! With the `msgpack` feature enabled, any type that can be (de-)serialized with serde/msgpack can be stored.
//!
//! The hash table consists of two parts:
//! 1) an actual hash table that stores the hash of the key and the position and size of the key/value data.
//! 2) a memory-managed data section where keys and values are stored.
//! Both parts grow and shrink automatically depending on usage.
//!
//! The used algorithms are optimized for performance so that the data storage should be faster that a regular
//! database.

use std::{cmp, fs::File, hash::Hasher, io, mem, path::Path};

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
/// Error type
pub enum Error {
    /// Any IO error
    Io(io::Error),
    /// The given file is not a valid table, as it has an invalid header
    WrongHeader,
    #[cfg(feature="msgpack")]
    /// A key or value could not be decoded
    Decode(rmp_serde::decode::Error),
    #[cfg(feature="msgpack")]
    /// A key or value could not be encoded
    Encode(rmp_serde::encode::Error)
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


/// A persistent hash table mapping key/value of type `&[u8]`.
/// 
/// This is the main struct of the crate. It manages two data structures:
/// 1) the "Index", a hash table containing the addresses of key/value data,
/// 2) and the data section, a memory managed area of data where all key/value data is actually stored.
/// 
/// The index uses a similar algorithm as [`std::collections::HashMap`], optimized for on-disc storage.
/// The hash algorithm is defined as SipHasher13 (which is also the default in Rust as of writing).
/// The index is automatically resized to keep its usage between 40% and 90%. This should keep the hash table efficient.
/// 
/// The data section uses B-Tree structures to track free and used data blocks in order to allocate and free memory regions in the data area.
/// This data section is extended when needed and shrinked (by moving data blocks to the front and truncating the free data at the end) 
/// whenever less than 50% of the data section is used.
pub struct Table {
    fd: File,
    mmap: MMap,
    header: &'static mut Header,
    index: Index,
    max_entries: usize,
    min_entries: usize,
    data: &'static mut [u8],
    data_start: u64,
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
        for entry in opened_fd.index_entries.iter_mut() {
            if entry.is_used() {
                if create {
                    entry.clear()
                } else {
                    mem.set_used(entry.data.position, entry.data.size, entry.hash);
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
            header: opened_fd.header,
            data: opened_fd.data,
            data_start: opened_fd.data_start as u64,
        };
        debug_assert!(tbl.is_valid(), "Inconsistent after creation");
        Ok(tbl)
    }

    #[inline]
    /// Open an existing table from the given path.
    ///
    /// Warning: Concurrent uses of the same table will result in data loss and other weird/unsafe behaviour.
    /// Make sure to only open the table once.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), false)
    }

    #[inline]
    /// Creates a new empty table. If the file exists, it will be overwritten.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), true)
    }

    pub(crate) fn allocate_data(&mut self, hash: Hash, mut size: u32) -> Result<u64, Error> {
        size = cmp::max(size, 1);
        match self.mem.allocate(size, hash) {
            Some(pos) => Ok(pos),
            None => {
                self.extend_data(size)?;
                Ok(self.mem.allocate(size, hash).expect("Still not enough space after extend"))
            }
        }
    }

    #[inline]
    pub(crate) fn free_data(&mut self, pos: u64) -> bool {
        self.mem.free(pos)
    }

    #[inline]
    pub(crate) fn get_data(&self, pos: u64, len: u32) -> &[u8] {
        if len == 0 {
            return &[];
        }
        debug_assert!(pos >= self.data_start);
        debug_assert!(pos + len as u64 <= self.data_start + self.data.len() as u64);
        &self.data[(pos - self.data_start) as usize..(pos + len as u64 - self.data_start) as usize]
    }

    #[inline]
    pub(crate) fn get_data_mut(&mut self, pos: u64, len: u32) -> &mut [u8] {
        if len == 0 {
            return &mut [];
        }
        debug_assert!(pos >= self.data_start);
        debug_assert!(pos + len as u64 <= self.data_start + self.data.len() as u64);
        &mut self.data[(pos - self.data_start) as usize..(pos + len as u64 - self.data_start) as usize]
    }

    #[inline]
    /// Returns the number of key/value pairs stored in the table.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    /// Returns the raw size of the table in bytes.
    pub fn size(&self) -> u64 {
        self.mmap.len() as u64
    }

    #[inline]
    /// Returns whether the table is empty
    pub fn is_empty(&self) -> bool {
        self.index.len() == 0
    }

    #[inline]
    /// Forces to write all pending changes to disk
    pub fn flush(&self) -> Result<(), Error> {
        self.mmap.flush().map_err(Error::Io)
    }

    #[inline]
    /// Retrieves and returns the value associated with the given key.
    /// If no entry with the given key is stored in the table, `None` is returned.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let hash = hash_key(key);
        if let Some(entry) = self.index.index_get(hash, |e| match_key(e, self.data, self.data_start, key)) {
            Some(self.get_data(entry.position + entry.key_size as u64, entry.size - entry.key_size as u32))
        } else {
            None
        }
    }

    #[inline]
    /// Retrieves and returns the value associated with the given key.
    /// If no entry with the given key is stored in the table, `None` is returned.
    /// If the returned data is modified, it directly affects the stored value.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut [u8]> {
        let hash = hash_key(key);
        if let Some(entry) = self.index.index_get(hash, |e| match_key(e, self.data, self.data_start, key)) {
            Some(self.get_data_mut(entry.position + entry.key_size as u64, entry.size - entry.key_size as u32))
        } else {
            None
        }
    }

    /// Stores the given key/value pair in the table.
    ///
    /// If another value is already stored for the key, this old entry will be removed from the table and returned.
    /// The returned reference is valid until another modification is made to the table.
    /// If the key is new ot the table, `None` is returned.
    ///
    /// Internally, a copy-on-write method is used instead of overwriting existing values. Therefore old values might
    /// be visible in the raw table file until a defragmentation happens.
    /// 
    /// This method might increase the size of the internal index or the data section as needed.
    /// If the table file cannot be extended (e.g. due to no space on device), the method will return an `Err` result.
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

    /// Deletes the entry with the given key
    /// 
    /// If an entry with the given key exists in the table, the entry is removed and a reference is returned.
    /// The returned reference is valid until another modification is made to the table.
    /// If the key is not found in the table, `None` is returned.
    ///
    /// Internally, deleted values are just marked as unused. Therefore old values might be visible in the 
    /// raw table file until a defragmentation happens.
    /// 
    /// This method might decrease the size of the internal index or the data section as needed.
    /// If the table file cannot be resized, the method will return an `Err` result.
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
    /// Deletes all entries in the table
    /// 
    /// This method essentially resets the table to its state after creation.
    pub fn clear(&mut self) -> Result<(), Error> {
        self.resize_fd(INITIAL_INDEX_CAPACITY, INITIAL_DATA_SIZE as u64)?;
        self.index.clear();
        self.mem.clear();
        self.header.index_capacity = INITIAL_INDEX_CAPACITY as u32;
        Ok(())
    }

    #[inline]
    /// Explicitly closes the table.
    /// 
    /// Normally this method does not need to be called.
    pub fn close(self) {
        // nothing to do, just drop self
    }

    pub(crate) fn is_valid(&self) -> bool {
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
            if entry.is_used()
                && entry.data.size > 0
                && !used.contains(&Used {
                    start: entry.data.position,
                    size: cmp::max(entry.data.size, 1),
                    hash: entry.hash,
                })
            {
                println!("Index entry at {} does not exist in mem", entry.data.position);
                valid = false;
            }
        }
        if used.len() != self.index.len() {
            println!("Index and data disagree about entry count: {} vs {}", self.index.len(), used.len());
            valid = false;
        }
        valid
    }
}
