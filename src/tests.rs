use std::{cmp, collections::HashMap, mem};

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

use crate::{
    index::IndexEntry,
    mmap::open_fd,
    table::{hash_key, Header},
    Table,
};

type Rand = ChaCha8Rng;

fn seeded_rng(s: u64) -> Rand {
    let mut seed: <Rand as SeedableRng>::Seed = Default::default();
    seed[0..8].copy_from_slice(&s.to_ne_bytes());
    Rand::from_seed(seed)
}

fn random_data(rand: &mut Rand, max_size: usize) -> Vec<u8> {
    let size = cmp::min(rand.gen_range(0..max_size), rand.gen_range(0..max_size));
    let mut data = vec![0; size];
    rand.fill_bytes(&mut data);
    data
}

#[test]
fn test_size() {
    assert_eq!(36, mem::size_of::<Header>());
    assert_eq!(24, mem::size_of::<IndexEntry>());
    assert_eq!(24576, mem::size_of::<[IndexEntry; 1024]>());
}

#[test]
fn test_hash() {
    assert_eq!(16183295663280961421, hash_key("test".as_bytes()));
}

#[test]
fn test_create_new() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let tbl = Table::create(file.path()).unwrap();
    assert!(tbl.is_valid());
}

#[test]
fn test_normal_use() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    tbl.set("key1".as_bytes(), "value1".as_bytes()).unwrap();
    tbl.set("key2".as_bytes(), "value2".as_bytes()).unwrap();
    assert!(tbl.is_valid());
    assert_eq!(tbl.len(), 2);
    assert_eq!(tbl.get("key1".as_bytes()), Some("value1".as_bytes()));
    assert_eq!(tbl.get("key2".as_bytes()), Some("value2".as_bytes()));
    tbl.set("key1".as_bytes(), "value3".as_bytes()).unwrap();
    assert!(tbl.is_valid());
    assert_eq!(tbl.len(), 2);
    assert_eq!(tbl.get("key1".as_bytes()), Some("value3".as_bytes()));
    assert_eq!(tbl.get("key2".as_bytes()), Some("value2".as_bytes()));
    assert!(tbl.delete("key1".as_bytes()).unwrap().is_some());
    assert!(tbl.delete("key2".as_bytes()).unwrap().is_some());
    assert!(tbl.is_valid());
    assert_eq!(tbl.len(), 0);
    assert_eq!(tbl.get("key1".as_bytes()), None);
    assert_eq!(tbl.get("key2".as_bytes()), None);
}

#[test]
fn test_zero_size() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    tbl.set(&[], &[]).unwrap();
    assert!(tbl.is_valid());
    assert_eq!(tbl.get(&[]), Some(&[] as &[u8]));
    tbl.set("no value".as_bytes(), &[]).unwrap();
    assert!(tbl.is_valid());
    assert_eq!(tbl.get("no value".as_bytes()), Some(&[] as &[u8]));
    tbl.set(&[], "no key".as_bytes()).unwrap();
    assert!(tbl.is_valid());
    assert_eq!(tbl.get(&[]), Some("no key".as_bytes()));
}

#[test]
fn test_endianness() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    tbl.set("key1".as_bytes(), "value1".as_bytes()).unwrap();
    let index = tbl.index.get_entries().iter().enumerate().find(|(_, entry)| entry.is_used()).unwrap().0;
    let hash = tbl.index.get_entries()[index].hash;
    tbl.close();
    {
        let mut tbl = open_fd(file.path(), false).unwrap();
        tbl.header.flags[0] = if tbl.header.flags[0] > 0 { 0 } else { 2 };
        tbl.header.fix_endianness();
        tbl.index_entries[index].fix_endianness();
        tbl.mmap.flush().unwrap();
    }
    let tbl = Table::open(file.path()).unwrap();
    assert_eq!(hash, tbl.index.get_entries()[index].hash);
    assert_eq!(tbl.get("key1".as_bytes()), Some("value1".as_bytes()));
}

fn test_one_seed(seed: u64) {
    let mut rand = seeded_rng(seed);
    let mut data = HashMap::new();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let count = rand.gen_range(100..1000);
    for _ in 0..count / 2 {
        let key = random_data(&mut rand, 100);
        let value = random_data(&mut rand, 1000);
        tbl.set(&key, &value).unwrap();
        assert!(tbl.is_valid());
        data.insert(key, value);
    }
    tbl.close();
    let mut tbl = Table::open(file.path()).unwrap();
    assert!(tbl.is_valid());
    for _ in count / 2..count {
        let key = random_data(&mut rand, 100);
        let value = random_data(&mut rand, 1000);
        tbl.set(&key, &value).unwrap();
        assert!(tbl.is_valid());
        data.insert(key, value);
    }
    tbl.close();
    let mut tbl = Table::open(file.path()).unwrap();
    assert!(tbl.is_valid());
    for (key, value) in data {
        let stored = tbl.get(&key);
        assert!(stored.is_some());
        assert_eq!(stored.unwrap(), &value);
        let removed = tbl.delete(&key).unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), &value);
        let stored = tbl.get(&key);
        assert!(stored.is_none());
        assert!(tbl.is_valid());
    }
    assert!(tbl.is_empty())
}

#[test]
fn smoke_test_42() {
    test_one_seed(42)
}

#[test]
fn smoke_test_666() {
    test_one_seed(666)
}

#[test]
fn smoke_test_1337() {
    test_one_seed(1337)
}

#[test]
fn smoke_test_1701() {
    test_one_seed(1701)
}

#[test]
#[ignore = "only for error search"]
fn search_for_error() {
    for seed in 0..10000 {
        println!("SEED = {}", seed);
        test_one_seed(seed)
    }
}
