use std::{cmp, collections::BTreeSet, ops::Bound};

use crate::Hash;

pub(crate) type Pos = u64;
pub(crate) type Size = u32;

#[derive(Ord, PartialEq, PartialOrd, Eq, Clone, Debug)]
pub struct Used {
    pub start: Pos,
    pub size: Size,
    pub hash: Hash,
}

impl Used {
    pub fn end(&self) -> Pos {
        self.start + self.size as Pos
    }
}

#[derive(Ord, PartialEq, PartialOrd, Eq, Clone, Debug)]
pub struct Free {
    pub size: Size,
    pub start: Pos,
}

impl Free {
    pub fn end(&self) -> Pos {
        self.start + self.size as Pos
    }
}

pub struct MemoryManagment {
    start: Pos,
    end: Pos,
    used: BTreeSet<Used>,
    free: BTreeSet<Free>,
    used_size: u64,
}

impl MemoryManagment {
    #[inline]
    pub fn new(start: Pos, end: Pos) -> Self {
        let mut free = BTreeSet::new();
        if start != end {
            free.insert(Free { start, size: (end - start) as Size });
        }
        Self { start, end, used: BTreeSet::new(), free, used_size: 0 }
    }

    #[inline]
    pub(crate) fn set_used(&mut self, start: Pos, size: Size, hash: Hash) {
        self.used.insert(Used { start, size: cmp::max(size, 1), hash });
    }

    pub(crate) fn fix_up(&mut self) {
        self.free.clear();
        self.used_size = 0;
        let mut last_end = self.start;
        for used in &self.used {
            self.used_size += used.size as u64;
            if used.start != last_end {
                self.free.insert(Free { size: (used.start - last_end) as Size, start: last_end });
            }
            last_end = used.end();
        }
        if last_end != self.end {
            self.free.insert(Free { size: (self.end - last_end) as Size, start: last_end });
        }
    }

    pub fn allocate(&mut self, mut size: Size, hash: Hash) -> Option<Pos> {
        size = cmp::max(size, 1);
        let candidates = self.free.range((Bound::Included(Free { size, start: 0 }), Bound::Unbounded)).take(5);
        let best = candidates.min_by_key(|cand| {
            (cand.size - size).next_power_of_two().trailing_zeros() + cand.start.next_power_of_two().trailing_zeros()
        });
        if let Some(free) = best.cloned() {
            assert!(self.free.remove(&free));
            debug_assert!(free.size >= size);
            if free.size > size {
                self.free.insert(Free { size: free.size - size, start: free.start + size as Pos });
            }
            self.used.insert(Used { start: free.start, size, hash });
            self.used_size += size as u64;
            Some(free.start)
        } else {
            None
        }
    }

    pub fn free(&mut self, pos: Pos) -> bool {
        let used = if let Some(used) = self
            .used
            .range((
                Bound::Included(Used { start: pos, size: 0, hash: 0 }),
                Bound::Excluded(Used { start: pos + 1, size: 0, hash: 0 }),
            ))
            .cloned()
            .next()
        {
            used
        } else {
            return false;
        };
        assert!(self.used.remove(&used));
        self.used_size -= used.size as u64;
        let mut free = Free { start: used.start, size: used.size };
        let free_before = if let Some(before) = self.used.range((Bound::Unbounded, Bound::Excluded(&used))).last() {
            Free { start: before.end(), size: (free.start - before.end()) as Size }
        } else {
            Free { start: self.start, size: (pos - self.start) as Size }
        };
        if free_before.size > 0 {
            assert!(self.free.remove(&free_before));
            free.start = free_before.start;
            free.size += free_before.size;
        }
        let free_after = if let Some(after) = self.used.range((Bound::Excluded(&used), Bound::Unbounded)).next() {
            Free { start: used.end(), size: (after.start - used.end()) as Size }
        } else {
            Free { start: used.end(), size: (self.end - used.end()) as Size }
        };
        if free_after.size > 0 {
            assert!(self.free.remove(&free_after));
            free.size += free_after.size;
        }
        self.free.insert(free);
        true
    }

    pub fn set_end(&mut self, end: Pos) -> Vec<Used> {
        let mut evicted = vec![];
        if end <= self.end {
            while let Some(last) = self.last_used().cloned() {
                if last.end() <= end {
                    break;
                }
                self.free(last.start);
                evicted.push(last);
            }
        }
        let mut last_free = if let Some(last) = self.last_used() {
            Free { start: last.end(), size: (self.end - last.end()) as Size }
        } else {
            Free { start: self.start, size: (self.end - self.start) as Size }
        };
        if last_free.size > 0 {
            assert!(self.free.remove(&last_free));
        }
        self.end = end;
        assert!(last_free.start <= self.end);
        last_free.size = (self.end - last_free.start) as Size;
        if last_free.size > 0 {
            self.free.insert(last_free);
        }
        evicted
    }

    pub fn set_start(&mut self, start: Pos) -> Vec<Used> {
        let mut evicted = vec![];
        if start > self.start {
            while let Some(first) = self.first_used().cloned() {
                if first.start >= start {
                    break;
                }
                self.free(first.start);
                evicted.push(first);
            }
        }
        let mut first_free = if let Some(first) = self.first_used() {
            Free { start: self.start, size: (first.start - self.start) as Size }
        } else {
            Free { start: self.start, size: (self.end - self.start) as Size }
        };
        if first_free.size > 0 {
            assert!(self.free.remove(&first_free));
        }
        self.start = start;
        assert!(first_free.end() >= self.start);
        first_free.size = (first_free.end() - self.start) as Size;
        first_free.start = self.start;
        if first_free.size > 0 {
            self.free.insert(first_free);
        }
        evicted
    }

    #[inline]
    fn first_used(&self) -> Option<&Used> {
        self.used.iter().next()
    }

    #[inline]
    fn last_used(&self) -> Option<&Used> {
        self.used.iter().last()
    }

    #[inline]
    pub(crate) fn get_used(&self) -> &BTreeSet<Used> {
        &self.used
    }

    #[inline]
    pub fn used_size(&self) -> u64 {
        self.used_size
    }

    #[inline]
    pub fn start(&self) -> Pos {
        self.start
    }

    #[inline]
    pub fn end(&self) -> Pos {
        self.end
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.used.clear();
        self.free.clear();
        self.used_size = 0;
    }

    #[inline]
    pub(crate) fn take_used(self) -> BTreeSet<Used> {
        self.used
    }

    pub(crate) fn is_valid(&self) -> bool {
        let mut valid = true;
        let mut blocks = Vec::with_capacity(self.used.len() + self.free.len());
        let mut used_size = 0;
        for used in &self.used {
            blocks.push((used.start, used.size, true));
            used_size += used.size as u64;
        }
        for free in &self.free {
            blocks.push((free.start, free.size, false))
        }
        if used_size != self.used_size {
            println!("Used size wrong: {} vs {}", used_size, self.used_size);
            valid = false;
        }
        if !blocks.is_empty() {
            blocks.sort_by_key(|&(p, ..)| p);
            let mut last = self.start;
            let mut used = !blocks[0].2;
            for &(p, l, u) in &blocks {
                if l == 0 {
                    println!("Zero-size block: (pos: {}, len:{}, used: {})", p, l, u);
                    valid = false;
                }
                if p != last || !u && !used {
                    println!(
                        "Non-sequential blocks: (end of last block: {}, used: {}) -> (pos: {}, len: {}, used: {})",
                        last, used, p, l, u
                    );
                    valid = false;
                }
                used = u;
                last = p + l as u64;
            }
            if last != self.end {
                println!("Last block does not end at end: {} vs {}", last, self.end);
                valid = false
            }
        }
        if !valid {
            println!("Start: {}, end: {}, used_size: {}", self.start, self.end, self.used_size);
            println!("Used: {:?}", self.used);
            println!("Free: {:?}", self.free);
        }
        valid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Op {
        Alloc { size: Size, hash: Hash, result: Option<Pos> },
        Free { pos: Pos, result: bool },
        SetStart { start: Pos, result: Vec<Used> },
        SetEnd { end: Pos, result: Vec<Used> },
    }

    #[cfg(test)]
    fn run_ops(mem: &mut MemoryManagment, ops: &[Op]) {
        assert!(mem.is_valid());
        for op in ops {
            println!("applying {:?}", op);
            match *op {
                Op::Alloc { size, hash, result } => assert_eq!(mem.allocate(size, hash), result),
                Op::Free { pos, result } => assert_eq!(mem.free(pos), result),
                Op::SetStart { start, ref result } => assert_eq!(&mem.set_start(start), result),
                Op::SetEnd { end, ref result } => assert_eq!(&mem.set_end(end), result),
            };
            assert!(mem.is_valid());
        }
    }

    #[test]
    fn allocate_free_sequential() {
        let mut mem = MemoryManagment::new(1000, 2000);
        run_ops(
            &mut mem,
            &[
                Op::Alloc { size: 100, hash: 0, result: Some(1000) },
                Op::Alloc { size: 200, hash: 0, result: Some(1100) },
                Op::Alloc { size: 400, hash: 0, result: Some(1300) },
                Op::Alloc { size: 800, hash: 0, result: None },
                Op::Alloc { size: 300, hash: 0, result: Some(1700) },
                Op::Alloc { size: 100, hash: 0, result: None },
                Op::Free { pos: 1000, result: true },
                Op::Free { pos: 1100, result: true },
                Op::Free { pos: 1300, result: true },
                Op::Free { pos: 1700, result: true },
            ],
        )
    }

    #[test]
    fn allocate_holes() {
        let mut mem = MemoryManagment::new(1000, 2000);
        run_ops(
            &mut mem,
            &[
                Op::Alloc { size: 400, hash: 0, result: Some(1000) },
                Op::Alloc { size: 100, hash: 0, result: Some(1400) },
                Op::Alloc { size: 300, hash: 0, result: Some(1500) },
                Op::Alloc { size: 100, hash: 0, result: Some(1800) },
                Op::Free { pos: 1000, result: true },
                Op::Free { pos: 1500, result: true },
                Op::Alloc { size: 350, hash: 0, result: Some(1000) },
                Op::Alloc { size: 200, hash: 0, result: Some(1500) },
                Op::Free { pos: 1400, result: true },
                Op::Free { pos: 1500, result: true },
                Op::Alloc { size: 400, hash: 0, result: Some(1350) },
            ],
        )
    }

    #[test]
    fn allocate_prefers_start() {
        let mut mem = MemoryManagment::new(1000, 2000);
        run_ops(
            &mut mem,
            &[
                Op::Alloc { size: 100, hash: 0, result: Some(1000) },
                Op::Alloc { size: 300, hash: 0, result: Some(1100) },
                Op::Alloc { size: 100, hash: 0, result: Some(1400) },
                Op::Alloc { size: 300, hash: 0, result: Some(1500) },
                Op::Alloc { size: 100, hash: 0, result: Some(1800) },
                Op::Free { pos: 1100, result: true },
                Op::Free { pos: 1500, result: true },
                Op::Alloc { size: 250, hash: 0, result: Some(1100) },
                Op::Alloc { size: 250, hash: 0, result: Some(1500) },
            ],
        )
    }

    #[test]
    fn allocate_prefers_better_fit() {
        let mut mem = MemoryManagment::new(1000, 2000);
        run_ops(
            &mut mem,
            &[
                Op::Alloc { size: 100, hash: 0, result: Some(1000) },
                Op::Alloc { size: 300, hash: 0, result: Some(1100) },
                Op::Alloc { size: 100, hash: 0, result: Some(1400) },
                Op::Alloc { size: 200, hash: 0, result: Some(1500) },
                Op::Alloc { size: 100, hash: 0, result: Some(1700) },
                Op::Free { pos: 1100, result: true },
                Op::Free { pos: 1500, result: true },
                Op::Alloc { size: 200, hash: 0, result: Some(1500) },
                Op::Alloc { size: 200, hash: 0, result: Some(1800) },
                Op::Alloc { size: 200, hash: 0, result: Some(1100) },
            ],
        )
    }

    #[test]
    fn increase_end() {
        let mut mem = MemoryManagment::new(1000, 2000);
        run_ops(
            &mut mem,
            &[
                Op::Alloc { size: 500, hash: 0, result: Some(1000) },
                Op::Alloc { size: 1000, hash: 0, result: None },
                Op::SetEnd { end: 3000, result: vec![] },
                Op::Alloc { size: 1000, hash: 0, result: Some(1500) },
            ],
        )
    }

    #[test]
    fn decrease_start() {
        let mut mem = MemoryManagment::new(1000, 2000);
        run_ops(
            &mut mem,
            &[
                Op::Alloc { size: 500, hash: 0, result: Some(1000) },
                Op::Alloc { size: 1000, hash: 0, result: None },
                Op::SetStart { start: 0, result: vec![] },
                Op::Alloc { size: 1000, hash: 0, result: Some(0) },
            ],
        )
    }
}
