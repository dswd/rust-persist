use std::{cmp, fs::File, hash::Hasher, mem, path::Path};

use siphasher::sip::SipHasher13;

use crate::memmngr::{MemoryManagment, Used};
use crate::{
    index::{Hash, Index, IndexEntry, IndexEntryData},
    mmap::{self, MMap},
    Error, INITIAL_DATA_SIZE, INITIAL_INDEX_CAPACITY, MAX_USAGE, MIN_USAGE,
};

#[inline(always)]
const fn is_be() -> bool {
    1001u16.to_be() == 1001u16
}

#[repr(C)]
pub(crate) struct Header {
    pub(crate) header: [u8; 16],
    pub(crate) flags: [u8; 16],
    pub(crate) index_capacity: u32,
}

impl Header {
    #[inline]
    pub fn set_flag(&mut self, byte: usize, bit: usize, val: bool) {
        let mask = 1u8 << bit;
        self.flags[byte] = (self.flags[byte] & !mask) | if val { mask } else { 0 }
    }

    #[inline]
    pub fn get_flag(&self, byte: usize, bit: usize) -> bool {
        let mask = 1u8 << bit;
        self.flags[byte] & mask > 0
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.get_flag(0, 0)
    }

    #[inline]
    pub fn set_dirty(&mut self, dirty: bool) {
        self.set_flag(0, 0, dirty)
    }

    #[inline]
    pub fn fix_endianness(&mut self) {
        self.index_capacity = self.index_capacity.to_be().to_le();
    }

    #[inline]
    pub fn has_correct_endianness(&self) -> bool {
        self.get_flag(0, 1) == is_be()
    }

    #[inline]
    pub fn set_correct_endianness(&mut self) {
        self.set_flag(0, 1, is_be())
    }
}

#[inline]
pub(crate) fn total_size(index_capacity: usize, data_size: u64) -> u64 {
    mem::size_of::<Header>() as u64 + index_capacity as u64 * mem::size_of::<IndexEntry>() as u64 + data_size
}

#[inline]
pub(crate) fn hash_key(key: &[u8]) -> Hash {
    let mut hasher = SipHasher13::default();
    hasher.write(key);
    hasher.finish()
}

#[inline]
fn match_key(entry: &IndexEntryData, data: &[u8], data_start: u64, key: &[u8]) -> bool {
    if key.is_empty() && entry.key_size == 0 {
        return true;
    }
    let start = (entry.position - data_start) as usize;
    let end = start + entry.key_size as usize;
    &data[start..end] == key
}

/// An entry in the table
pub struct Entry<'a> {
    /// Flags stored with the entry
    pub flags: u16,

    /// The key of the entry
    pub key: &'a [u8],

    /// The value of the entry
    pub value: &'a [u8],
}

/// An entry in the table with mutable value
pub struct EntryMut<'a> {
    /// Flags stored with the entry
    ///
    /// Modifications to this field are not reflected in the table
    pub flags: u16,

    /// The key of the entry
    pub key: &'a [u8],

    /// The value of the entry
    ///
    /// Modifications to this value are reflected in the table
    pub value: &'a mut [u8],
}

/// A persistent hash table mapping key/value of type `&[u8]`.
///
/// This is the main struct of the crate. It manages two data structures:
/// 1) the "Index", a hash table containing the addresses of key/value data,
/// 2) and the data section, a memory managed area of data where all key/value data is actually stored.
///
/// The index uses a similar algorithm as [`std::collections::HashMap`], optimized for on-disc storage.
/// The hash algorithm is defined as SipHasher13 (which is also the default in Rust as of writing).
/// The index is automatically resized to keep its usage between 35% and 90%. This should keep the hash table efficient.
///
/// The data section uses B-Tree structures to track free and used data blocks in order to allocate and free memory regions in the data area.
/// This data section is extended when needed and shrinked (by moving data blocks to the front and truncating the free data at the end)
/// whenever less than 50% of the data section is used.
pub struct Table {
    pub(crate) fd: File,
    pub(crate) mmap: MMap,
    pub(crate) header: &'static mut Header,
    pub(crate) index: Index,
    pub(crate) max_entries: usize,
    pub(crate) min_entries: usize,
    pub(crate) data: &'static mut [u8],
    pub(crate) data_start: u64,
    pub(crate) mem: MemoryManagment,
}

impl Table {
    fn new_index(path: &Path, create: bool) -> Result<Self, Error> {
        let opened_fd = mmap::open_fd(path, create)?;
        let mut mem = MemoryManagment::new(
            opened_fd.data_start as u64,
            opened_fd.data_start as u64 + opened_fd.data.len() as u64,
        );
        if !opened_fd.header.has_correct_endianness() {
            for entry in opened_fd.index_entries.iter_mut() {
                entry.fix_endianness()
            }
            opened_fd.header.fix_endianness();
            opened_fd.header.set_correct_endianness();
        }
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

    /// Open an existing table from the given path.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), false)
    }

    /// Creates a new empty table. If the file exists, it will be overwritten.
    #[inline]
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), true)
    }

    /// Opens an existing or creates a new typed table at the given path.
    #[inline]
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        if path.exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
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

    /// Returns the number of key/value pairs stored in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns the raw size of the table in bytes.
    #[inline]
    pub fn size(&self) -> u64 {
        self.mmap.len() as u64
    }

    /// Returns whether the table is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.index.len() == 0
    }

    /// Forces to write all pending changes to disk
    #[inline]
    pub fn flush(&self) -> Result<(), Error> {
        self.mmap.flush().map_err(Error::Io)
    }

    #[inline]
    pub(crate) fn entry_from_index_data(&self, entry: IndexEntryData) -> Entry<'_> {
        let data = self.get_data(entry.position, entry.size);
        let (key, value) = data.split_at(entry.key_size as usize);
        Entry { key, value, flags: entry.flags }
    }

    #[inline]
    pub(crate) fn entry_mut_from_index_data(&mut self, entry: IndexEntryData) -> EntryMut<'_> {
        let data = self.get_data_mut(entry.position, entry.size);
        let (key, value) = data.split_at_mut(entry.key_size as usize);
        EntryMut { key, value, flags: entry.flags }
    }

    /// Returns whether an entry is associated with the given key.
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        let hash = hash_key(key);
        self.index.index_get(hash, |e| match_key(e, self.data, self.data_start, key)).is_some()
    }

    /// Retrieves and returns the entry associated with the given key.
    /// If no entry with the given key is stored in the table, `None` is returned.
    #[inline]
    pub fn get_entry(&self, key: &[u8]) -> Option<Entry<'_>> {
        let hash = hash_key(key);
        self.index
            .index_get(hash, |e| match_key(e, self.data, self.data_start, key))
            .map(|e| self.entry_from_index_data(e))
    }

    /// Retrieves and returns the value associated with the given key.
    /// If no entry with the given key is stored in the table, `None` is returned.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.get_entry(key).map(|e| e.value)
    }

    /// Retrieves and returns the entry associated with the given key.
    /// If no entry with the given key is stored in the table, `None` is returned.
    /// If the returned value is modified, it directly affects the stored value.
    #[inline]
    pub fn get_entry_mut(&mut self, key: &[u8]) -> Option<EntryMut<'_>> {
        let hash = hash_key(key);
        self.index
            .index_get(hash, |e| match_key(e, self.data, self.data_start, key))
            .map(move |entry| self.entry_mut_from_index_data(entry))
    }

    /// Retrieves and returns the value associated with the given key.
    /// If no entry with the given key is stored in the table, `None` is returned.
    /// If the returned value is modified, it directly affects the stored value.
    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut [u8]> {
        self.get_entry_mut(key).map(|e| e.value)
    }

    /// Stores the given entry in the table.
    ///
    /// If another entry is already stored for the key, this old entry will be removed from the table and returned.
    /// The returned reference is valid until another modification is made to the table.
    /// If the key is new ot the table, `None` is returned.
    ///
    /// Internally, a copy-on-write method is used instead of overwriting existing values. Therefore old values might
    /// be visible in the raw table file until a defragmentation happens.
    ///
    /// This method might increase the size of the internal index or the data section as needed.
    /// If the table file cannot be extended (e.g. due to no space on device), the method will return an `Err` result.
    #[inline]
    pub fn set_entry<'a>(&mut self, entry: Entry<'a>) -> Result<Option<EntryMut<'_>>, Error> {
        self.maybe_extend_index()?;
        self.maybe_shrink_data()?;
        let hash = hash_key(entry.key);
        let len = (entry.key.len() + entry.value.len()) as u32;
        let pos = self.allocate_data(hash, len)?;
        if len > 0 {
            let space = self.get_data_mut(pos, len);
            space[..entry.key.len()].copy_from_slice(entry.key);
            space[entry.key.len()..].copy_from_slice(entry.value);
        }
        let index_entry =
            IndexEntryData { position: pos, size: len, key_size: entry.key.len() as u16, flags: entry.flags };
        let result = {
            let data = &self.data;
            let data_start = self.data_start;
            self.index.index_set(hash, |e| match_key(e, data, data_start, entry.key), index_entry)
        };
        match result {
            Some(old) => {
                self.free_data(old.position);
                Ok(Some(self.entry_mut_from_index_data(old)))
            }
            None => Ok(None),
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
    #[inline]
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.set_entry(Entry { key, value, flags: 0 }).map(|r| r.map(|e| e.value))
    }

    /// Deletes the entry with the given key
    ///
    /// If an entry with the given key exists in the table, the entry is removed and returned.
    /// The returned reference is valid until another modification is made to the table.
    /// If the key is not found in the table, `None` is returned.
    ///
    /// Internally, deleted values are just marked as unused. Therefore old values might be visible in the
    /// raw table file until a defragmentation happens.
    ///
    /// This method might decrease the size of the internal index or the data section as needed.
    /// If the table file cannot be resized, the method will return an `Err` result.
    #[inline]
    pub fn delete_entry(&mut self, key: &[u8]) -> Result<Option<EntryMut<'_>>, Error> {
        self.maybe_shrink_index()?;
        self.maybe_shrink_data()?;
        Ok(self.delete_entry_no_shrink(key))
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
    #[inline]
    pub fn delete(&mut self, key: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.delete_entry(key).map(|r| r.map(|e| e.value))
    }

    #[inline]
    pub(crate) fn delete_entry_no_shrink<'a>(&'a mut self, key: &[u8]) -> Option<EntryMut<'a>> {
        let hash = hash_key(key);
        let result = {
            let data = &self.data;
            let data_start = self.data_start;
            self.index.index_delete(hash, |e| match_key(e, data, data_start, key))
        };
        match result {
            Some(old) => {
                self.free_data(old.position);
                Some(self.entry_mut_from_index_data(old))
            }
            None => None,
        }
    }

    /// Deletes all entries in the table
    ///
    /// This method essentially resets the table to its state after creation.
    #[inline]
    pub fn clear(&mut self) -> Result<(), Error> {
        self.resize_fd(INITIAL_INDEX_CAPACITY, INITIAL_DATA_SIZE as u64)?;
        self.index.clear();
        self.mem.clear();
        self.header.index_capacity = INITIAL_INDEX_CAPACITY as u32;
        Ok(())
    }

    /// Explicitly closes the table.
    ///
    /// Normally this method does not need to be called.
    #[inline]
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

    /// Return a statistics struct
    pub fn stats(&self) -> Stats {
        Stats {
            valid: self.is_valid(),
            entries: self.len(),
            size: self.size(),
            hash_size: self.index.capacity() as u64 * mem::size_of::<IndexEntry>() as u64,
            hash_free: (self.index.capacity() - self.index.len()) as u64 * mem::size_of::<IndexEntry>() as u64,
            data_size: self.mem.end() - self.mem.start(),
            data_free: self.mem.end() - self.mem.start() - self.mem.used_size(),
        }
    }
}


/// Struct containing table statistics
#[derive(Debug)]
pub struct Stats {
    /// Whether the table is valid/consistent
    pub valid: bool,

    /// Entries contained in the table
    pub entries: usize,

    /// Total byte size of the table
    pub size: u64,

    /// Total size of the hash table part
    pub hash_size: u64,

    /// Free size of the hash table part
    pub hash_free: u64,

    /// Total size of the data part
    pub data_size: u64,

    /// Free size of the table part
    pub data_free: u64,
}
