# Idea

General layout:
* Header
* Index hashtable
* Data blobs

## Header

* Magic header: rust-persist-01\n
* Index size: u32

## Index for Hashtable

Entry fields:
- Hash of Key: u64
- Size of data + Flags: u32
- Position in data: u64 (position from start of file)

Algorithm: Robin hood hashing, stealing

## Data blobs

### In-Memory data structure to store blocks
* (position, size) of free blocks sorted by size and then position
* (position, size) of used blocks sorted by position

### Free block selection
Best match: log2(free space after) + log2(position) of the first 3 fitting free blocks

### Optimization
* Normal: Move blobs into existing gaps to the left
* Aggressive: Move all blobs to the left to close all gaps
* Automatic normal optimization when used space < 50%

## Core functionality

### Load file
- Scan the whole index
- Rearrange index entries that are not positioned correctly
- Build up in-memory structures

### Get data
- Hash key
- Find key in index -> return None if not found
- Go to data at location
- Verify keys match -> otherwise continue in index
- Return handle for data

### Remove data
- Hash key
- Find key in index -> return None if not found
- Remove index entry, backshift if needed
- Optimize if needed
- Remove block from used blocks
- Add to free blocks
- Return handle to existing data

### Add data
- Find free block, extend if needed
- Write data to block
- Remove free block, add block as used
- Hash key
- Add key to index, steal if needed
- If existing key, remove that data

### Optimize
If index usage > 90%
- Move all data blocks out of the way
- Increase index size internally
- Reinsert index entries
If index usage < 35%
- Decrease index size internally
- Reinsert index entries
- Declare seconds index half as free space
If used block space < 50%
- Start from back of the data
- Move all blocks to free block in the front
- Do multiple iterations
- Shrink data part if possible

## API

get(&self, key: &[u8]) -> Option<&[u8]>
get_mut(&mut self, key: &[u8]) -> Option<&mut [u8]>
set(&mut self, key: &[u8], value: &[u8]) -> Option<&mut [u8]>
delete(&mut self, key: &[u8]) -> Option<&mut [u8]>
clear(&mut self)

iter(&'a self) -> Iter<'a>
iter_mut(&'a mut self) -> IterMut<'a>
each(&self, f: FnMut(&[u8], &[u8]))
each_mut(&mut self, f: FnMut(&[u8], &mut[u8]))
filter(&mut self, f: FnMut(&[u8], &[u8]) -> bool)
optimize(&mut self, aggressive: bool)

### Higher level APIs

* MessagePack
* Compression