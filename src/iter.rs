use crate::{index::Entry, Database, Error};

pub struct Iter<'a> {
    pos: usize,
    entries: &'a [Entry],
    db: &'a Database,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

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
            let data = self.db.get_data(entry.data.position, entry.data.size);
            let (key, value) = data.split_at(entry.data.key_size as usize);
            return Some((key, value));
        }
    }
}



impl Database {
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        Iter { pos: 0, entries: self.index.get_entries(), db: self }
    }

    pub fn each<F: FnMut(&[u8], &[u8])>(&self, mut f: F) {
        for (k, v) in self.iter() {
            f(k, v)
        }
    }

    pub fn each_mut<F: FnMut(&[u8], &mut [u8])>(&mut self, mut f: F) {
        for pos in 0..self.index.capacity() {
            let entry_data = {
                let entry = &self.index.get_entries()[pos];
                if !entry.is_used() {
                    continue;
                }
                entry.data
            };
            let data = self.get_data_mut(entry_data.position, entry_data.size);
            let (key, value) = data.split_at_mut(entry_data.key_size as usize);
            f(key, value)
        }
    }

    pub fn filter<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut f: F) -> Result<(), Error> {
        let mut pos = 0;
        loop {
            if pos >= self.index.capacity() {
                return Ok(())
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
                if f(key, value) {
                    pos += 1;
                    continue;
                }
                key.to_vec()
            };
            self.delete(&key)?;
        }
    }
}


#[test]
fn test_iter() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::create(file.path()).unwrap();
    db.set("key1".as_bytes(), "value1".as_bytes()).unwrap();
    db.set("key2".as_bytes(), "value2".as_bytes()).unwrap();
    assert_eq!(db.iter().count(), 2);
}