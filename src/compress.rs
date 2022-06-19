use std::{marker::PhantomData, path::Path};

use serde::{Serialize, de::DeserializeOwned};

use crate::{Entry, Error, Table, Stats, serialize, deserialize};

/// Method used internally to compress data
#[inline]
pub fn compress(val: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(val)
}

/// Method used internally to decompress data
#[inline]
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, Error> {
    lz4_flex::decompress_size_prepended(data).map_err(Error::Decompress)
}

impl Table {
    /// Loads and returns the compressed value stored with the given key.
    ///
    /// If no entry with the given key exists in the table, `None` is returned.
    /// If the key cannot be encoded or the value cannot be decoded or decompressed, `Err` is returned.
    ///
    /// See [TypedTable](TypedTable#on-serialization) for more info on serialization.
    #[inline]
    pub fn get_compressed_obj<K: Serialize, V: DeserializeOwned>(&self, key: K) -> Result<Option<V>, Error> {
        match self.get(&serialize(key)?) {
            Some(v) => Ok(Some(deserialize(&decompress(v)?)?)),
            None => Ok(None),
        }
    }

    /// Stores the given key/value pair in the table and compresses the value.
    ///
    /// Returns whether the key has already been in the table (and the value has been overwritten).
    /// If the key cannot be encoded or the value cannot be encoded, `Err` is returned.
    ///
    /// This method might increase the size of the internal index or the data section as needed.
    /// If the table file cannot be extended (e.g. due to no space on device), the method will return an `Err` result.
    ///
    /// See [TypedTable](TypedTable#on-serialization) for more info on serialization.
    #[inline]
    pub fn set_compressed_obj<K: Serialize, V: Serialize>(&mut self, key: K, value: V) -> Result<bool, Error> {
        self.set(&serialize(key)?, &compress(&serialize(value)?)).map(|v| v.is_some())
    }

    /// Deletes and returns the entry with the given key from the table.
    ///
    /// If no entry with the given key exists in the table, `None` is returned.
    /// If the key cannot be encoded or the value cannot be decoded, `Err` is returned.
    ///
    /// This method might decrease the size of the internal index or the data section as needed.
    /// If the table file cannot be resized, the method will return an `Err` result.
    ///
    /// See [TypedTable](TypedTable#on-serialization) for more info on serialization.
    #[inline]
    pub fn take_compressed_obj<K: Serialize, V: DeserializeOwned>(&mut self, key: K) -> Result<Option<V>, Error> {
        match self.delete(&serialize(key)?)? {
            Some(v) => Ok(Some(deserialize(&decompress(v)?)?)),
            None => Ok(None),
        }
    }
}


/// Internal iterator over all entries in the typed table
struct Iter<K, V, I> {
    inner: I,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned, I: Iterator<Item = Entry<'a>>> Iterator for Iter<K, V, I> {
    type Item = Result<(K, V), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| Ok((deserialize(entry.key)?, deserialize(&decompress(entry.value)?)?)))
    }
}


/// Internal iterator over all keys in the typed table
struct KeyIter<K, I> {
    inner: I,
    _key: PhantomData<K>,
}

impl<'a, K: DeserializeOwned, I: Iterator<Item = Entry<'a>>> Iterator for KeyIter<K, I> {
    type Item = Result<K, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| deserialize(entry.key))
    }
}


/// A typed version of the table with compressed values.
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
pub struct CompressedTypedTable<K, V> {
    inner: Table,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> CompressedTypedTable<K, V> {
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

    /// Opens an existing or creates a new typed table at the given path.
    #[inline]
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        if path.exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    /// Returns a reference to the wrapped [`Table`].
    /// 
    /// Beware that the inner table will expose the raw compressed data
    #[inline]
    pub fn inner(&self) -> &Table {
        &self.inner
    }

    /// Returns the wrapped [`Table`].
    /// 
    /// Beware that the inner table will expose the raw compressed data
    #[inline]
    pub fn into_inner(self) -> Table {
        self.inner
    }

    /// Returns whether an entry is associated with the given key.
    #[inline]
    pub fn contains(&self, key: &K) -> Result<bool, Error> {
        self.inner.contains_obj(key)
    }

    /// Loads and returns the value stored with the given key.
    ///
    /// See [`Table::get_obj`] for more info
    #[inline]
    pub fn get(&self, key: &K) -> Result<Option<V>, Error> {
        self.inner.get_compressed_obj(key)
    }

    /// Stores the given key/value pair in the table.
    ///
    /// See [`Table::set_obj`] for more info
    #[inline]
    pub fn set(&mut self, key: &K, value: &V) -> Result<bool, Error> {
        self.inner.set_compressed_obj(key, value)
    }

    /// Deletes the entry with the given key from the table.
    ///
    /// See [`Table::delete_obj`] for more info
    #[inline]
    pub fn delete(&mut self, key: &K) -> Result<bool, Error> {
        self.inner.delete_obj(key)
    }

    /// Deletes and return the entry with the given key from the table.
    ///
    /// See [`Table::take_obj`] for more info
    #[inline]
    pub fn take(&mut self, key: &K) -> Result<Option<V>, Error> {
        self.inner.take_compressed_obj(key)
    }


    /// Iterate over all entries in the typed table
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = Result<(K, V), Error>> + '_ {
        Iter { inner: self.inner.iter(), _key: PhantomData, _value: PhantomData }
    }

    /// Iterate over all entries in the typed table
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = Result<K, Error>> + '_ {
        KeyIter { inner: self.inner.iter(), _key: PhantomData }
    }

    /// Return the number of entries in the table
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return the raw size of the table in bytes
    #[inline]
    pub fn size(&self) -> u64 {
        self.inner.size() as u64
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

    /// Return a statistics struct
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

}