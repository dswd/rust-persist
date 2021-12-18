use std::fs::OpenOptions;
use std::path::Path;
use std::{fs::File, mem, slice};

use fs2::FileExt;
use memmap::MmapMut;

pub type MMap = MmapMut;

use crate::{total_size, IndexEntry, Error, Header, INDEX_HEADER, INITIAL_DATA_SIZE, INITIAL_INDEX_CAPACITY};

/// This method is unsafe as it potentially creates references to uninitialized memory
pub(crate) unsafe fn mmap_as_ref(
    mmap: &mut MMap, index_capacity: usize,
) -> (&'static mut Header, &'static mut [IndexEntry], usize, &'static mut [u8]) {
    if (mmap.len() as u64) < total_size(index_capacity, 0) {
        panic!("Memory map too small");
    }
    let header = &mut *(mmap.as_mut_ptr() as *mut Header);
    let ptr = mmap.as_mut_ptr().add(mem::size_of::<Header>()) as *mut IndexEntry;
    let entries = slice::from_raw_parts_mut(ptr, index_capacity);
    let data_start = total_size(index_capacity, 0) as usize;
    let data = slice::from_raw_parts_mut(mmap.as_mut_ptr().add(data_start), mmap.len() - data_start);
    (header, entries, data_start, data)
}

pub(crate) fn map_fd(fd: &File) -> Result<MMap, Error> {
    unsafe {
        MMap::map_mut(fd).map_err(Error::Io)
    }
}

pub(crate) struct OpenFdResult {
    pub fd: File,
    pub mmap: MMap,
    pub header: &'static mut Header,
    pub index_entries: &'static mut [IndexEntry],
    pub data_start: usize,
    pub data: &'static mut [u8],
}

pub(crate) fn open_fd(path: &Path, create: bool) -> Result<OpenFdResult, Error> {
    let fd = OpenOptions::new().read(true).write(true).create(create).open(path).map_err(Error::Io)?;
    fd.lock_exclusive().map_err(Error::Io)?;
    if create {
        fd.set_len(total_size(INITIAL_INDEX_CAPACITY, INITIAL_DATA_SIZE as u64)).map_err(Error::Io)?;
    }
    let mut mmap = map_fd(&fd)?;
    if mmap.len() < mem::size_of::<Header>() {
        return Err(Error::WrongHeader);
    }
    let (header, ..) = unsafe { mmap_as_ref(&mut mmap, INITIAL_INDEX_CAPACITY as usize) };
    if create {
        // This is safe, nothing in header is Drop
        header.header = INDEX_HEADER;
        header.index_capacity = INITIAL_INDEX_CAPACITY as u32;
        header.set_correct_endianness();
    }
    if header.header != INDEX_HEADER {
        return Err(Error::WrongHeader);
    }
    let (header, index_entries, data_start, data) = unsafe { mmap_as_ref(&mut mmap, header.index_capacity as usize) };
    Ok(OpenFdResult { fd, mmap, header, index_entries, data_start, data })
}
