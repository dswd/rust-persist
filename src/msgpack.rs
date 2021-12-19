use std::{marker::PhantomData, path::Path};

use serde::{de::DeserializeOwned, Serialize};

use crate::{Entry, Error, Table};

/// Method used internally to serialize values to bytes
#[inline]
pub fn serialize<T: Serialize>(val: T) -> Result<Vec<u8>, Error> {
    rmp_serde::to_vec(&val).map_err(Error::Serialize)
}

/// Method used internally to deserialize values from bytes
#[inline]
pub fn deserialize<T: DeserializeOwned>(data: &[u8]) -> Result<T, Error> {
    rmp_serde::from_read(data).map_err(Error::Deserialize)
}

impl Table {
    /// Loads and returns the value stored with the given key.
    ///
    /// If no entry with the given key exists in the table, `None` is returned.
    /// If the key cannot be encoded or the value cannot be decoded, `Err` is returned.
    ///
    /// See [TypedTable](TypedTable#on-serialization) for more info on serialization.
    #[inline]
    pub fn get_obj<K: Serialize, V: DeserializeOwned>(&self, key: K) -> Result<Option<V>, Error> {
        match self.get(&serialize(key)?) {
            Some(v) => Ok(Some(deserialize(v)?)),
            None => Ok(None),
        }
    }

    /// Stores the given key/value pair in the table.
    ///
    /// Returns whether the key has already been in the table (and the value has been overwritten).
    /// If the key cannot be encoded or the value cannot be encoded, `Err` is returned.
    ///
    /// This method might increase the size of the internal index or the data section as needed.
    /// If the table file cannot be extended (e.g. due to no space on device), the method will return an `Err` result.
    ///
    /// See [TypedTable](TypedTable#on-serialization) for more info on serialization.
    #[inline]
    pub fn set_obj<K: Serialize, V: Serialize>(&mut self, key: K, value: V) -> Result<bool, Error> {
        self.set(&serialize(key)?, &serialize(value)?).map(|v| v.is_some())
    }

    /// Deletes the entry with the given key from the table.
    ///
    /// Returns whether the key has been in the table or not.
    /// If the key cannot be encoded, `Err` is returned.
    ///
    /// This method might decrease the size of the internal index or the data section as needed.
    /// If the table file cannot be resized, the method will return an `Err` result.
    ///
    /// See [TypedTable](TypedTable#on-serialization) for more info on serialization.
    #[inline]
    pub fn delete_obj<K: Serialize>(&mut self, key: K) -> Result<bool, Error> {
        self.delete(&serialize(key)?).map(|v| v.is_some())
    }
}

/// Internal iterator over all entries in the typed table
pub struct Iter<K, V, I> {
    inner: I,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned, I: Iterator<Item = Entry<'a>>> Iterator for Iter<K, V, I> {
    type Item = Result<(K, V), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| Ok((deserialize(entry.key)?, deserialize(entry.value)?)))
    }
}

/// A typed version of the table.
///
/// This struct wraps the normal [`Table`] and ensures that keys and values have a certain type.
///
/// ## On serialization
///
/// This functionality requires the feature `msgpack`.
///
/// For encoding/decoding data, the MessagePack format is used. In particular,
/// a compressed encoding is used that skips the field names and encodes structs as arrays.
/// Please see [`serde`], [`rmp_serde`] and [MessagePack](https://msgpack.org) for more info.
///
/// If you want to enable serialization for custom structs and types, you can either implement
/// [`serde::Serialize`] and [`serde::Deserialize`] directly or use [the `derive` feature of `serde`](https://serde.rs/derive.html).
///
/// If any key or value cannot be encoded or decoded, [`Error::Serialize`] or [`Error::Deserialize`] is thrown.
pub struct TypedTable<K, V> {
    inner: Table,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> TypedTable<K, V> {
    /// Opens an existing typed table from the given path.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self { inner: Table::open(path)?, _key: PhantomData, _value: PhantomData })
    }

    /// Creates a new typed table at the given path (overwriting an existing table).
    #[inline]
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self { inner: Table::create(path)?, _key: PhantomData, _value: PhantomData })
    }

    /// Returns a reference to the wrapped [`Table`].
    #[inline]
    pub fn inner(&self) -> &Table {
        &self.inner
    }

    /// Returns the wrapped [`Table`].
    #[inline]
    pub fn into_inner(self) -> Table {
        self.inner
    }

    /// Loads and returns the value stored with the given key.
    ///
    /// See [`Table::get_obj`] for more info
    #[inline]
    pub fn get(&self, key: K) -> Result<Option<V>, Error> {
        self.inner.get_obj(key)
    }

    /// Stores the given key/value pair in the table.
    ///
    /// See [`Table::set_obj`] for more info
    #[inline]
    pub fn set(&mut self, key: K, value: V) -> Result<bool, Error> {
        self.inner.set_obj(key, value)
    }

    /// Deletes the entry with the given key from the table.
    ///
    /// See [`Table::delete_obj`] for more info
    #[inline]
    pub fn delete(&mut self, key: K) -> Result<bool, Error> {
        self.inner.delete_obj(key)
    }

    /// Iterate over all entries in the typed table
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = Result<(K, V), Error>> + '_ {
        Iter { inner: self.inner.iter(), _key: PhantomData, _value: PhantomData }
    }

    /// Return the number of entries in the table
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return the raw size of the table in bytes
    #[inline]
    pub fn size(&self) -> u64 {
        self.inner.len() as u64
    }

    /// Return whether the table is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    /// Forces to write all pending changes to disk
    #[inline]
    pub fn flush(&mut self) -> Result<(), Error> {
        self.inner.flush()
    }

    /// Forces defragmentation of the data section.
    ///
    /// See [`Table::defragment`] for more info.
    #[inline]
    pub fn defragment(&mut self) -> Result<(), Error> {
        self.inner.defragment()
    }

    /// Explicitly closes the table.
    ///
    /// Normally this method does not need to be called.
    #[inline]
    pub fn close(self) {
        self.inner.close()
    }

    /// Deletes all entries in the table
    ///
    /// This method essentially resets the table to its state after creation.
    #[inline]
    pub fn clear(&mut self) -> Result<(), Error> {
        self.inner.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_types() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        tbl.set_obj("key1", "value1").unwrap();
        tbl.set_obj(("key2", 1), (1, true)).unwrap();
        assert!(tbl.is_valid());
        assert_eq!(tbl.len(), 2);
        assert_eq!(tbl.get_obj("key1").unwrap(), Some("value1".to_string()));
        assert_eq!(tbl.get_obj(("key2", 1)).unwrap(), Some((1, true)));
        tbl.set_obj("key1", "value3").unwrap();
        assert!(tbl.is_valid());
        assert_eq!(tbl.len(), 2);
        assert_eq!(tbl.get_obj("key1").unwrap(), Some("value3".to_string()));
        assert_eq!(tbl.get_obj(("key2", 1)).unwrap(), Some((1, true)));
        assert!(tbl.delete_obj("key1").unwrap());
        assert!(tbl.delete_obj(("key2", 1)).unwrap());
        assert!(tbl.is_valid());
        assert_eq!(tbl.len(), 0);
        assert_eq!(tbl.get_obj("key1").unwrap(), Option::<bool>::None);
        assert_eq!(tbl.get_obj(("key2", 1)).unwrap(), Option::<bool>::None);
    }

    #[test]
    fn test_static_types() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut tbl = TypedTable::<usize, String>::create(file.path()).unwrap();
        tbl.set(1, "value1".to_string()).unwrap();
        tbl.set(2, "value2".to_string()).unwrap();
        assert!(tbl.inner().is_valid());
        assert_eq!(tbl.len(), 2);
        assert_eq!(tbl.get(1).unwrap(), Some("value1".to_string()));
        assert_eq!(tbl.get(2).unwrap(), Some("value2".to_string()));
        tbl.set(1, "value3".to_string()).unwrap();
        assert!(tbl.inner().is_valid());
        assert_eq!(tbl.len(), 2);
        assert_eq!(tbl.get(1).unwrap(), Some("value3".to_string()));
        assert_eq!(tbl.get(2).unwrap(), Some("value2".to_string()));
        assert!(tbl.delete(1).unwrap());
        assert!(tbl.delete(2).unwrap());
        assert!(tbl.inner().is_valid());
        assert_eq!(tbl.len(), 0);
        assert_eq!(tbl.get(1).unwrap(), None);
        assert_eq!(tbl.get(2).unwrap(), None);
    }

    #[test]
    fn test_static_iter() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut tbl = TypedTable::<usize, String>::create(file.path()).unwrap();
        tbl.set(1, "value1".to_string()).unwrap();
        tbl.set(2, "value2".to_string()).unwrap();
        assert_eq!(tbl.iter().count(), 2);
    }
}
