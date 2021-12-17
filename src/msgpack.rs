use std::{marker::PhantomData, path::Path};

use serde::{de::DeserializeOwned, Serialize};

use crate::{Database, Error};

impl Database {
    pub fn get_obj<K: Serialize, V: DeserializeOwned>(&self, k: K) -> Option<V> {
        let key = rmp_serde::to_vec(&k).expect("Failed to encode");
        let value = self.get(&key);
        value.map(|v| rmp_serde::from_read(v).expect("Failed to decode"))
    }

    pub fn set_obj<K: Serialize, V: Serialize>(&mut self, k: K, v: V) -> Result<bool, Error> {
        let key = rmp_serde::to_vec(&k).expect("Failed to encode");
        let value = rmp_serde::to_vec(&v).expect("Failed to encode");
        self.set(&key, &value).map(|v| v.is_some())
    }

    pub fn delete_obj<K: Serialize>(&mut self, k: K) -> Result<bool, Error> {
        let key = rmp_serde::to_vec(&k).expect("Failed to encode");
        self.delete(&key).map(|v| v.is_some())
    }
}

pub struct Iter<K, V, I> {
    inner: I,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned, I: Iterator<Item=(&'a [u8], &'a[u8])>> Iterator for Iter<K, V, I> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (rmp_serde::from_read(k).expect("Failed to decode"), rmp_serde::from_read(v).expect("Failed to decode"))
        })
    }
}

pub struct TypedDatabase<K, V> {
    inner: Database,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> TypedDatabase<K, V> {
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self { inner: Database::open(path)?, _key: PhantomData, _value: PhantomData })
    }

    #[inline]
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self { inner: Database::create(path)?, _key: PhantomData, _value: PhantomData })
    }

    #[inline]
    pub fn inner(&self) -> &Database {
        &self.inner
    }

    #[inline]
    pub fn into_inner(self) -> Database {
        self.inner
    }

    #[inline]
    pub fn get(&self, key: K) -> Option<V> {
        self.inner.get_obj(key)
    }

    #[inline]
    pub fn set(&mut self, key: K, value: V) -> Result<bool, Error> {
        self.inner.set_obj(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: K) -> Result<bool, Error> {
        self.inner.delete_obj(key)
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item=(K, V)> + '_ {
        Iter {
            inner: self.inner.iter(),
            _key: PhantomData,
            _value: PhantomData
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.inner.len() as u64
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }
}


#[test]
fn test_dynamic_types() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::create(file.path()).unwrap();
    db.set_obj("key1", "value1").unwrap();
    db.set_obj(("key2", 1), (1, true)).unwrap();
    assert!(db.is_valid());
    assert_eq!(db.len(), 2);
    assert_eq!(db.get_obj("key1"), Some("value1".to_string()));
    assert_eq!(db.get_obj(("key2", 1)), Some((1, true)));
    db.set_obj("key1", "value3").unwrap();
    assert!(db.is_valid());
    assert_eq!(db.len(), 2);
    assert_eq!(db.get_obj("key1"), Some("value3".to_string()));
    assert_eq!(db.get_obj(("key2", 1)), Some((1, true)));
    assert!(db.delete_obj("key1").unwrap());
    assert!(db.delete_obj(("key2", 1)).unwrap());
    assert!(db.is_valid());
    assert_eq!(db.len(), 0);
    assert_eq!(db.get_obj("key1"), Option::<bool>::None);
    assert_eq!(db.get_obj(("key2", 1)), Option::<bool>::None);
}

#[test]
fn test_static_types() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut db = TypedDatabase::<usize, String>::create(file.path()).unwrap();
    db.set(1, "value1".to_string()).unwrap();
    db.set(2, "value2".to_string()).unwrap();
    assert!(db.inner().is_valid());
    assert_eq!(db.len(), 2);
    assert_eq!(db.get(1), Some("value1".to_string()));
    assert_eq!(db.get(2), Some("value2".to_string()));
    db.set(1, "value3".to_string()).unwrap();
    assert!(db.inner().is_valid());
    assert_eq!(db.len(), 2);
    assert_eq!(db.get(1), Some("value3".to_string()));
    assert_eq!(db.get(2), Some("value2".to_string()));
    assert!(db.delete(1).unwrap());
    assert!(db.delete(2).unwrap());
    assert!(db.inner().is_valid());
    assert_eq!(db.len(), 0);
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), None);
}

#[test]
fn test_static_iter() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut db = TypedDatabase::<usize, String>::create(file.path()).unwrap();
    db.set(1, "value1".to_string()).unwrap();
    db.set(2, "value2".to_string()).unwrap();
    assert_eq!(db.iter().count(), 2);
}