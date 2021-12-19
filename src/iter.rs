use crate::{index::IndexEntry, Entry, EntryMut, Error, Table};

/// Internal iterator over all entries in a table
pub struct Iter<'a> {
    pos: usize,
    entries: &'a [IndexEntry],
    tbl: &'a Table,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos >= self.entries.len() {
                return None;
            }
            let entry = &self.entries[self.pos];
            self.pos += 1;
            if !entry.is_used() {
                continue;
            }
            return Some(self.tbl.entry_from_index_data(entry.data));
        }
    }
}

impl Table {
    /// Returns an iterator over all entries in the table
    ///
    /// Each entry will be returned exactly once but in no particular order.
    /// The entries are returned as tuples of key and value.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = Entry<'_>> {
        Iter { pos: 0, entries: self.index.get_entries(), tbl: self }
    }

    /// Execute the given method for all entries in the table
    ///
    /// The method will be executed once for each entry in the table.
    #[inline]
    pub fn each<F: FnMut(Entry<'_>)>(&self, mut f: F) {
        for entry in self.iter() {
            f(entry)
        }
    }

    /// Execute the given method for all entries in the table
    ///
    /// The method will be executed once for each entry in the table.
    /// Changes to the values will be directy reflected in the table.
    pub fn each_mut<F: FnMut(EntryMut<'_>)>(&mut self, mut f: F) {
        for pos in 0..self.index.capacity() {
            let entry_data = {
                let entry = &self.index.get_entries()[pos];
                if !entry.is_used() {
                    continue;
                }
                entry.data
            };
            f(self.entry_mut_from_index_data(entry_data))
        }
    }

    /// Filters the entries in the table according to the given predicate.
    ///
    /// If the predicate `f` returns `true` for a key/value pair, the entry will remain in the table, otherwise it will be removed.
    pub fn filter<F: FnMut(Entry<'_>) -> bool>(&mut self, mut f: F) -> Result<(), Error> {
        let mut pos = 0;
        loop {
            if pos >= self.index.capacity() {
                break;
            }
            let entry_data = {
                let entry = &self.index.get_entries()[pos];
                if !entry.is_used() {
                    pos += 1;
                    continue;
                }
                entry.data
            };
            let key = {
                let data = self.get_data(entry_data.position, entry_data.size);
                let (key, value) = data.split_at(entry_data.key_size as usize);
                if f(Entry { key, value, flags: entry_data.flags }) {
                    pos += 1;
                    continue;
                }
                key.to_vec()
            };
            self.delete_entry_no_shrink(&key);
        }
        self.maybe_shrink_index()?;
        self.maybe_shrink_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iter() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        tbl.set("key1".as_bytes(), "value1".as_bytes()).unwrap();
        tbl.set("key2".as_bytes(), "value2".as_bytes()).unwrap();
        assert_eq!(tbl.iter().count(), 2);
    }
}
