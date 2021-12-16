use std::{collections::hash_map::DefaultHasher, fs::File, hash::Hasher, io, mem, path::Path};

use index::{Entry, EntryData, Hash, Index};
use memmngr::MemoryManagment;
use mmap::MemoryMap;

mod index;
mod resize;
mod memmngr;
mod mmap_io;

const INDEX_HEADER: [u8; 16] = *b"rust-persist-01\n";

const MAX_USAGE: f64 = 0.9;
const MIN_USAGE: f64 = 0.35;
const INITIAL_INDEX_CAPACITY: usize = 128;
const INITIAL_DATA_SIZE: usize = 0;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Mmap(mmap::MapError),
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

pub struct Database {
    fd: File,
    mmap: MemoryMap,
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
        let (fd, mmap, header, entries, data_start, data) = mmap_io::open_fd(path, create)?;
        let mut mem = MemoryManagment::new(data_start as u64, data_start as u64 + data.len() as u64);
        let mut count = 0;
        for entry in entries.iter() {
            if entry.is_used() {
                mem.set_used(entry.data.position, entry.data.size, entry.hash);
                count += 1;
            }
        }
        mem.fix_up();
        let mut index = Index::new(entries, count);
        if header.is_dirty() {
            index.reinsert_all();
            assert!(index.is_valid(), "Inconsistent after reinsert");
            header.set_dirty(false);
        }
        let db = Self {
            max_entries: (header.index_capacity as f64 * MAX_USAGE) as usize,
            min_entries: (header.index_capacity as f64 * MIN_USAGE) as usize,
            fd,
            mmap,
            index,
            mem,
            header,
            data,
            data_start: data_start as u64,
        };
        debug_assert!(db.is_valid(), "Inconsistent after creation");
        Ok(db)
    }

    #[inline]
    pub fn open(path: &Path) -> Result<Self, Error> {
        Self::new_index(path, false)
    }

    #[inline]
    pub fn create(path: &Path) -> Result<Self, Error> {
        Self::new_index(path, true)
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

    pub(crate) fn free_data(&mut self, pos: u64) -> Result<(), Error> {
        self.maybe_shrink_data()?;
        self.mem.free(pos);
        Ok(())
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
        if let Some(entry) = self
            .index
            .index_get(hash, |e| &self.data[e.position as usize..e.position as usize + e.key_size as usize] == key)
        {
            Some(self.get_data(entry.position + entry.key_size as u64, entry.size - entry.key_size as u32))
        } else {
            None
        }
    }

    #[inline]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut [u8]> {
        let hash = hash_key(key);
        if let Some(entry) = self
            .index
            .index_get(hash, |e| &self.data[e.position as usize..e.position as usize + e.key_size as usize] == key)
        {
            Some(self.get_data_mut(entry.position + entry.key_size as u64, entry.size - entry.key_size as u32))
        } else {
            None
        }
    }

    #[inline]
    pub fn set(&mut self, key: &[u8], data: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.maybe_extend_index()?;
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
            self.index.index_set(
                hash,
                |e| &data[e.position as usize..e.position as usize + e.key_size as usize] == key,
                entry,
            )
        };
        if let Some(old) = result {
            self.free_data(old.position)?;
            let old_data = self.get_data_mut(old.position + old.key_size as u64, old.size - old.key_size as u32);
            Ok(Some(old_data))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn delete(&mut self, key: &[u8]) -> Result<Option<&mut [u8]>, Error> {
        self.maybe_shrink_index()?;
        let hash = hash_key(key);
        let result = {
            let data = &self.data;
            self.index
                .index_delete(hash, |e| &data[e.position as usize..e.position as usize + e.key_size as usize] == key)
        };
        if let Some(old) = result {
            self.free_data(old.position)?;
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

    pub fn is_valid(&self) -> bool {
        self.index.is_valid() && self.mem.is_valid()
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
    let db = Database::create(Path::new("test.db")).unwrap();
    assert!(db.is_valid());
}
