use std::mem;

pub(crate) type Hash = u64;

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct EntryData {
    pub position: u64,
    pub size: u32,
    pub key_size: u16,
    pub flags: u16,
}

#[repr(C)]
pub(crate) struct Entry {
    pub(crate) hash: Hash,
    pub(crate) data: EntryData,
}

impl Entry {
    #[inline]
    pub(crate) fn is_used(&self) -> bool {
        self.hash != 0
    }

    #[inline]
    fn clear(&mut self) {
        self.hash = 0
    }
}

#[derive(Debug)]
pub enum LocateResult {
    Found(usize), // Found the key at this position
    Hole(usize),  // Found a hole at this position while searching for a key
    Steal(usize), // Found a spot to steal at this position while searching for a key
}

pub struct Index {
    mask: usize,
    capacity: usize,
    count: usize,
    entries: &'static mut [Entry],
}

impl Index {
    pub(crate) fn new(entries: &'static mut [Entry], used_count: usize) -> Self {
        let capacity = entries.len();
        debug_assert_eq!(capacity.count_ones(), 1);
        Self { mask: capacity - 1, capacity, count: used_count, entries }
    }

    fn reinsert(&mut self, start: usize, end: usize) {
        for pos in start..end {
            let hash;
            let data;
            {
                let entry = &mut self.entries[pos];
                if !entry.is_used() {
                    continue;
                }
                hash = entry.hash;
                data = entry.data;
                entry.clear();
            }
            self.count -= 1;
            self.index_set(hash, |_| false, data);
        }
    }

    pub(crate) fn grow_from_half(&mut self) {
        self.reinsert(0, self.capacity/2)
    }

    pub(crate) fn shrink_to_half(&mut self) {
        assert!(self.count <= self.capacity/2);
        self.capacity /= 2;
        self.mask = self.capacity-1;
        self.reinsert(self.capacity, 2*self.capacity);
    }

    pub(crate) fn reinsert_all(&mut self) {
        self.reinsert(0, self.capacity)
    }

    pub(crate) fn clear(&mut self) {
        for entry in self.entries.iter_mut() {
            entry.clear()
        }
        self.count = 0;
    }

    pub(crate) fn update_block_position(&mut self, hash: Hash, old_pos: u64, new_pos: u64) {
        let mut pos = (hash & self.mask as u64) as usize;
        loop {
            let entry = &mut self.entries[pos];
            if !entry.is_used() {
                return;
            }
            if entry.hash == hash && entry.data.position == old_pos {
                entry.data.position = new_pos;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn get_displacement(&self, entry: &Entry, pos: usize) -> usize {
        (pos + self.capacity - (entry.hash as usize & self.mask)) & self.mask
    }

    /// Finds the position for this key
    /// If the key is in the table, it will be the position of the key,
    /// otherwise it will be the position where this key should be inserted
    pub(crate) fn locate<F: FnMut(&EntryData) -> bool>(&self, hash: Hash, mut match_fn: F) -> LocateResult {
        let mut pos = (hash & self.mask as u64) as usize;
        let mut dist = 0;
        loop {
            let entry = &self.entries[pos];
            if !entry.is_used() {
                return LocateResult::Hole(pos);
            }
            if entry.hash == hash && match_fn(&entry.data) {
                return LocateResult::Found(pos);
            }
            let odist = self.get_displacement(entry, pos);
            if dist > odist {
                return LocateResult::Steal(pos);
            }
            pos = (pos + 1) & self.mask;
            dist += 1;
        }
    }

    /// Shifts all following entries towards the left if they can get closer to their ideal position.
    /// The entry at the given position will be lost.
    fn backshift(&mut self, start: usize) {
        let mut pos = start;
        let mut last_pos;
        loop {
            last_pos = pos;
            pos = (pos + 1) & self.mask;
            {
                let entry = &self.entries[pos];
                if !entry.is_used() {
                    // we found a hole, stop shifting here
                    break;
                }
                if (entry.hash & self.mask as u64) as usize == pos {
                    // we found an entry at the right position, stop shifting here
                    break;
                }
            }
            self.entries.swap(last_pos, pos);
        }
        self.entries[last_pos].clear();
    }

    pub(crate) fn index_set<F: FnMut(&EntryData) -> bool>(
        &mut self, hash: Hash, match_fn: F, data: EntryData,
    ) -> Option<EntryData> {
        match self.locate(hash, match_fn) {
            LocateResult::Found(pos) => {
                let mut old = data;
                mem::swap(&mut old, &mut self.entries[pos].data);
                Some(old)
            }
            LocateResult::Hole(pos) => {
                let entry = &mut self.entries[pos];
                entry.hash = hash;
                entry.data = data;
                self.count += 1;
                None
            }
            LocateResult::Steal(pos) => {
                let mut stolen_key;
                let mut stolen_data;
                let mut cur_pos = pos;
                {
                    let entry = &mut self.entries[pos];
                    stolen_key = entry.hash;
                    stolen_data = entry.data;
                    entry.hash = hash;
                    entry.data = data;
                }
                loop {
                    cur_pos = (cur_pos + 1) & self.mask;
                    let entry = &mut self.entries[cur_pos];
                    if entry.is_used() {
                        mem::swap(&mut stolen_key, &mut entry.hash);
                        mem::swap(&mut stolen_data, &mut entry.data);
                    } else {
                        entry.hash = stolen_key;
                        entry.data = stolen_data;
                        break;
                    }
                }
                self.count += 1;
                None
            }
        }
    }

    #[inline]
    pub(crate) fn index_get<F: FnMut(&EntryData) -> bool>(&self, hash: Hash, match_fn: F) -> Option<EntryData> {
        match self.locate(hash, match_fn) {
            LocateResult::Found(pos) => Some(self.entries[pos].data),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn index_delete<F: FnMut(&EntryData) -> bool>(&mut self, hash: Hash, match_fn: F) -> Option<EntryData> {
        match self.locate(hash, match_fn) {
            LocateResult::Found(pos) => {
                let entry = self.entries[pos].data;
                self.backshift(pos);
                self.count -= 1;
                Some(entry)
            }
            _ => None,
        }
    }

    pub fn is_valid(&self) -> bool {
        let mut valid = true;
        let mut entries = 0;
        for pos in 0..self.capacity {
            let entry = &self.entries[pos];
            if !entry.is_used() {
                continue;
            }
            entries += 1;
            match self.locate(entry.hash, |_| true) {
                LocateResult::Found(p) if p == pos => (),
                found => {
                    println!("Index error: entry is at wrong position, expected: {}, actual: {:?}", pos, found);
                    valid = false;
                }
            };
        }
        if entries != self.count {
            println!("Index error: entry count does not match, expected: {}, actual: {}", self.count, entries);
            valid = false;
        }
        valid
    }
}
