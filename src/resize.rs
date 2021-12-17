use std::mem;

use crate::{
    index::Index,
    memmngr::MemoryManagment,
    mmap::{self, mmap_as_ref},
    total_size, Error, Table, INITIAL_INDEX_CAPACITY, MAX_USAGE, MIN_USAGE,
};

impl Table {
    pub(crate) fn resize_fd(&mut self, index_capacity: usize, data_size: u64) -> Result<(), Error> {
        self.flush()?;
        self.fd.set_len(total_size(index_capacity, data_size)).map_err(Error::Io)?;
        self.mmap = mmap::map_fd(&self.fd)?;
        let (header, entries, data_start, data) = unsafe { mmap_as_ref(&mut self.mmap, index_capacity) };
        self.header = header;
        self.data = data;
        self.data_start = data_start as u64;
        self.index = Index::new(entries, self.index.len());
        self.min_entries = (index_capacity as f64 * MIN_USAGE) as usize;
        self.max_entries = (index_capacity as f64 * MAX_USAGE) as usize;
        Ok(())
    }

    pub(crate) fn extend_data(&mut self, size: u32) -> Result<(), Error> {
        debug_assert!(self.is_valid(), "Invalid before extend data");
        self.resize_fd(self.index.capacity(), (self.data.len() + size as usize) as u64)?;
        assert!(self.mem.set_end(self.data_start + self.data.len() as u64).is_empty());
        debug_assert!(self.is_valid(), "Invalid after extend data");
        Ok(())
    }

    /// Forces the defragmentation of the data section.
    /// 
    /// This method will move all data chunks to the front and remove all gaps between them.
    /// After this, the free space at the end will be truncated to save space.
    /// 
    /// This method is automatically called when the used space of the data section is less than 50%
    pub fn defragment(&mut self) -> Result<(), Error> {
        debug_assert!(self.is_valid(), "Invalid before shrink data");
        let mut old_mem = MemoryManagment::new(self.mem.start(), self.mem.end());
        mem::swap(&mut self.mem, &mut old_mem);
        for old_entry in old_mem.take_used() {
            let new_pos =
                self.mem.allocate(old_entry.size, old_entry.hash).expect("Defragmented bigger than fragmented");
            safemem::copy_over(
                self.data,
                (old_entry.start - self.data_start) as usize,
                (new_pos - self.data_start) as usize,
                old_entry.size as usize,
            );
            self.index.update_block_position(old_entry.hash, old_entry.start, new_pos);
        }
        self.resize_fd(self.index.capacity(), self.mem.used_size())?;
        assert!(self.mem.set_end(self.data_start + self.data.len() as u64).is_empty());
        debug_assert!(self.is_valid(), "Invalid after shrink data");
        Ok(())
    }

    pub(crate) fn maybe_shrink_data(&mut self) -> Result<(), Error> {
        if self.mem.used_size() > self.data.len() as u64 / 2 || self.data.len() <= 4 * 1024 {
            return Ok(());
        }
        self.defragment()
    }

    pub(crate) fn maybe_extend_index(&mut self) -> Result<(), Error> {
        if self.index.len() <= self.max_entries {
            return Ok(());
        }
        debug_assert!(self.is_valid(), "Invalid before extend index");
        self.header.set_dirty(true);
        let index_capacity_new = self.index.capacity() * 2;
        let data_start_new = total_size(index_capacity_new, 0);
        let evicted = self.mem.set_start(data_start_new);
        // important: begin with last evicted block to avoid overwriting its second half with the first entry
        for old_entry in evicted.into_iter().rev() {
            let new_pos = match self.mem.allocate(old_entry.size, old_entry.hash) {
                Some(pos) => pos,
                None => {
                    self.resize_fd(self.index.capacity(), (self.data.len() + old_entry.size as usize) as u64)?;
                    assert!(self.mem.set_end(self.data_start + self.data.len() as u64).is_empty());
                    self.mem.allocate(old_entry.size, old_entry.hash).expect("Not big enough after extending")
                }
            };
            safemem::copy_over(
                self.data,
                (old_entry.start - self.data_start) as usize,
                (new_pos - self.data_start) as usize,
                old_entry.size as usize,
            );
            self.index.update_block_position(old_entry.hash, old_entry.start, new_pos);
        }
        debug_assert!(self.is_valid(), "Invalid middle extend index");
        self.header.index_capacity = index_capacity_new as u32;
        let data_size_new = self.mem.end() - self.mem.start();
        self.resize_fd(index_capacity_new, data_size_new)?;
        assert!(self.mem.set_end(self.data_start + self.data.len() as u64).is_empty());
        self.index.grow_from_half();
        self.header.set_dirty(false);
        debug_assert!(self.is_valid(), "Invalid after extend index");
        Ok(())
    }

    pub(crate) fn maybe_shrink_index(&mut self) -> Result<bool, Error> {
        if self.index.len() >= self.min_entries || self.index.capacity() <= INITIAL_INDEX_CAPACITY {
            return Ok(false);
        }
        debug_assert!(self.is_valid(), "Invalid before shrink index");
        self.header.set_dirty(true);
        let index_capacity_new = self.index.capacity() / 2;
        let data_start_new = total_size(index_capacity_new, 0);
        self.index.shrink_to_half();
        debug_assert!(self.is_valid(), "Invalid middle shrink index");
        self.header.index_capacity = index_capacity_new as u32;
        assert!(self.mem.set_start(data_start_new).is_empty());
        let data_size_new = self.mem.end() - self.mem.start();
        self.resize_fd(index_capacity_new, data_size_new)?;
        assert_eq!(self.data_start, data_start_new);
        self.header.set_dirty(false);
        debug_assert!(self.is_valid(), "Invalid after shrink index");
        Ok(true)
    }
}

#[test]
fn extend_data() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let key1 = [0; 1024];
    let key2 = [1; 1024];
    let data = [0; 1024 * 10];
    tbl.set(&key1, &data).unwrap();
    assert!(tbl.is_valid());
    tbl.set(&key2, &data).unwrap();
    assert!(tbl.is_valid());
    tbl.close();
    let tbl = Table::open(file.path()).unwrap();
    assert!(tbl.is_valid());
}

#[test]
fn shrink_data() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let key = [0; 1024];
    let data = [0; 1024 * 10];
    tbl.set(&key, &data).unwrap();
    assert!(tbl.is_valid());
    assert!(tbl.delete(&key).unwrap().is_some());
    assert!(tbl.is_valid());
    tbl.close();
    let tbl = Table::open(file.path()).unwrap();
    assert!(tbl.is_valid());
}

#[test]
fn extend_index() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let data = [0; 100];
    for i in 0u16..150 {
        tbl.set(&i.to_ne_bytes(), &data).unwrap();
        assert!(tbl.is_valid());
    }
    assert!(tbl.index.capacity() > INITIAL_INDEX_CAPACITY);
    tbl.close();
    let tbl = Table::open(file.path()).unwrap();
    assert!(tbl.is_valid());
}

#[test]
fn shrink_index() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let data = [0; 100];
    for i in 0u16..150 {
        tbl.set(&i.to_ne_bytes(), &data).unwrap();
    }
    assert!(tbl.is_valid());
    assert!(tbl.index.capacity() > INITIAL_INDEX_CAPACITY);
    for i in 0u16..150 {
        tbl.delete(&i.to_ne_bytes()).unwrap();
    }
    assert!(tbl.index.capacity() == INITIAL_INDEX_CAPACITY);
    tbl.close();
    let tbl = Table::open(file.path()).unwrap();
    assert!(tbl.is_valid());
}
