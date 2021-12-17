use std::{collections::hash_map::DefaultHasher, fs::File, hash::Hasher, io, mem, path::Path};

use index::{Entry, EntryData, Hash, Index};
use memmngr::{MemoryManagment, Used};
use mmap_io::MMap;

mod index;
mod memmngr;
mod mmap_io;
mod resize;
#[cfg(test)]
mod tests;

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
    pub fn is_dirty(&self) -> bool {
        self.flags[0] & 1 == 1
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.flags[0] = self.flags[0] & 0xfe | if dirty { 1 } else { 0 }
    }
}

fn total_size(index_capacity: usize, data_size: u64) -> u64 {
    mem::size_of::<Header>() as u64 + index_capacity as u64 * mem::size_of::<Entry>() as u64 + data_size
}

fn hash_key(key: &[u8]) -> Hash {
    let mut hasher = DefaultHasher::default();
    hasher.write(key);
    hasher.finish()
}

fn match_key(entry: &EntryData, data: &[u8], data_start: u64, key: &[u8]) -> bool {
    let start = (entry.position - data_start) as usize;
    let end = start + entry.key_size as usize;
    &data[start..end] == key
}

pub struct Database {
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

impl Database {
    pub fn new_index(path: &Path, create: bool) -> Result<Self, Error> {
        let opened_fd = mmap_io::open_fd(path, create)?;
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
        let db = Self {
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
        debug_assert!(db.is_valid(), "Inconsistent after creation");
        Ok(db)
    }

    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), false)
    }

    #[inline]
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Self::new_index(path.as_ref(), true)
    }

    pub(crate) fn allocate_data(&mut self, hash: Hash, size: u32) -> Result<u64, Error> {
        match self.mem.allocate(size, hash) {
            Some(pos) => Ok(pos),
            None => {
                self.extend_data(size)?;
                Ok(self.mem.allocate(size, hash).expect("Still not enough space after extend"))
            }
        }
    }

    pub(crate) fn get_data(&self, pos: u64, len: u32) -> &[u8] {
        debug_assert!(pos >= self.data_start);
        debug_assert!(pos + len as u64 <= self.data_start + self.data.len() as u64);
        &self.data[(pos - self.data_start) as usize..(pos + len as u64 - self.data_start) as usize]
    }

    pub(crate) fn get_data_mut(&mut self, pos: u64, len: u32) -> &mut [u8] {
        debug_assert!(pos >= self.data_start);
        debug_assert!(pos + len as u64 <= self.data_start + self.data.len() as u64);
        &mut self.data[(pos - self.data_start) as usize..(pos + len as u64 - self.data_start) as usize]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.mmap.len() as u64
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.index.len() == 0
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

    #[inline]
    pub fn set(&mut self, key: &[u8], data: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.maybe_extend_index()?;
        self.maybe_shrink_data()?;
        let hash = hash_key(key);
        let len = (key.len() + data.len()) as u32;
        let pos = self.allocate_data(hash, len)?;
        {
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
            self.mem.free(old.position);
            let old_data = self.get_data_mut(old.position + old.key_size as u64, old.size - old.key_size as u32);
            Ok(Some(old_data))
        } else {
            Ok(None)
        }
    }

    #[inline]
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
            self.mem.free(old.position);
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
        self.header.index_capacity = INITIAL_INDEX_CAPACITY as u32;
        Ok(())
    }

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
            if entry.is_used()
                && !used.contains(&Used { start: entry.data.position, size: entry.data.size, hash: entry.hash })
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

#[test]
fn test_size() {
    assert_eq!(36, mem::size_of::<Header>());
    assert_eq!(24, mem::size_of::<Entry>());
    assert_eq!(24576, mem::size_of::<[Entry; 1024]>());
}

#[test]
fn test_create_new() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let db = Database::create(file.path()).unwrap();
    assert!(db.is_valid());
}

#[test]
fn test_normal_use() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::create(file.path()).unwrap();
    db.set("key1".as_bytes(), "value1".as_bytes()).unwrap();
    db.set("key2".as_bytes(), "value2".as_bytes()).unwrap();
    assert!(db.is_valid());
    assert_eq!(db.len(), 2);
    assert_eq!(db.get("key1".as_bytes()), Some("value1".as_bytes()));
    assert_eq!(db.get("key2".as_bytes()), Some("value2".as_bytes()));
    db.set("key1".as_bytes(), "value3".as_bytes()).unwrap();
    assert!(db.is_valid());
    assert_eq!(db.len(), 2);
    assert_eq!(db.get("key1".as_bytes()), Some("value3".as_bytes()));
    assert_eq!(db.get("key2".as_bytes()), Some("value2".as_bytes()));
    assert!(db.delete("key1".as_bytes()).unwrap().is_some());
    assert!(db.delete("key2".as_bytes()).unwrap().is_some());
    assert!(db.is_valid());
    assert_eq!(db.len(), 0);
    assert_eq!(db.get("key1".as_bytes()), None);
    assert_eq!(db.get("key2".as_bytes()), None);
}