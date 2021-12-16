use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::{fs::File, mem, slice};

use mmap::{MapOption, MemoryMap};

use crate::{total_size, Entry, Error, Header, INDEX_HEADER, INITIAL_DATA_SIZE, INITIAL_INDEX_CAPACITY, Database};

/// This method is unsafe as it potentially creates references to uninitialized memory
pub(crate) unsafe fn mmap_as_ref(
    mmap: &MemoryMap, index_capacity: usize,
) -> (&'static mut Header, &'static mut [Entry], usize, &'static mut [u8]) {
    if (mmap.len() as u64) < total_size(index_capacity, 0) {
        panic!("Memory map too small");
    }
    let header = &mut *(mmap.data() as *mut Header);
    let ptr = mmap.data().add(mem::size_of::<Header>()) as *mut Entry;
    let entries = slice::from_raw_parts_mut(ptr, index_capacity);
    let data_start = total_size(index_capacity, 0) as usize;
    let data = slice::from_raw_parts_mut(mmap.data().add(data_start), mmap.len() - data_start);
    (header, entries, data_start, data)
}

pub(crate) fn map_fd(fd: &File) -> Result<MemoryMap, Error> {
    MemoryMap::new(
        fd.metadata().map_err(Error::Io)?.len() as usize,
        &[
            MapOption::MapReadable,
            MapOption::MapWritable,
            MapOption::MapFd(fd.as_raw_fd()),
            MapOption::MapNonStandardFlags(0x0001), // libc::consts::os::posix88::MAP_SHARED
        ],
    )
    .map_err(Error::Mmap)
}

pub(crate) fn open_fd(
    path: &Path, create: bool,
) -> Result<(File, MemoryMap, &'static mut Header, &'static mut [Entry], usize, &'static mut [u8]), Error> {
    let fd = OpenOptions::new().read(true).write(true).create(create).open(path).map_err(Error::Io)?;
    if create {
        fd.set_len(total_size(INITIAL_INDEX_CAPACITY, INITIAL_DATA_SIZE as u64)).map_err(Error::Io)?;
    }
    let mmap = map_fd(&fd)?;
    if mmap.len() < mem::size_of::<Header>() {
        return Err(Error::WrongHeader);
    }
    let (header, ..) = unsafe { mmap_as_ref(&mmap, INITIAL_INDEX_CAPACITY as usize) };
    if create {
        // This is safe, nothing in header is Drop
        header.header = INDEX_HEADER;
        header.index_capacity = INITIAL_INDEX_CAPACITY as u32;
    }
    if header.header != INDEX_HEADER {
        return Err(Error::WrongHeader);
    }
    let (header, entries, data_start, data) = unsafe { mmap_as_ref(&mmap, header.index_capacity as usize) };
    Ok((fd, mmap, header, entries, data_start, data))
}