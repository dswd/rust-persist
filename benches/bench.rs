use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_persist::Table;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Table::set (key 1 byte, value 1 bytes)", |b| {
        let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        let key = &[0];
        let value = &[0];
        b.iter(|| tbl.set(black_box(key), black_box(value)).is_ok())
    });
    c.bench_function("Table::set (key 10 bytes, value 100 bytes)", |b| {
        let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        let key = &[0u8; 10];
        let value = &[0u8; 100];
        b.iter(|| tbl.set(black_box(key), black_box(value)).is_ok())
    });
    c.bench_function("Table::set (key 100 bytes, value 1000 bytes)", |b| {
        let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        let key = &[0u8; 100];
        let value = &[0u8; 1000];
        b.iter(|| tbl.set(black_box(key), black_box(value)).is_ok())
    });
    c.bench_function("HashMap::insert (key 1 bytes, value 1 bytes)", |b| {
        let mut hashmap = HashMap::with_capacity(100);
        let key = vec![0u8; 1];
        let value = vec![0u8; 1];
        b.iter(|| hashmap.insert(black_box(key.clone()), black_box(value.clone())))
    });
    c.bench_function("HashMap::insert (key 10 bytes, value 100 bytes)", |b| {
        let mut hashmap = HashMap::with_capacity(100);
        let key = vec![0u8; 10];
        let value = vec![0u8; 100];
        b.iter(|| hashmap.insert(black_box(key.clone()), black_box(value.clone())))
    });
    c.bench_function("HashMap::insert (key 100 bytes, value 1000 bytes)", |b| {
        let mut hashmap = HashMap::with_capacity(100);
        let key = vec![0u8; 100];
        let value = vec![0u8; 1000];
        b.iter(|| hashmap.insert(black_box(key.clone()), black_box(value.clone())))
    });
    c.bench_function("Table::get (key 1 byte, value 1 bytes)", |b| {
        let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        let key = &[0];
        let value = &[0];
        tbl.set(key, value).unwrap();
        b.iter(|| tbl.get(black_box(key)))
    });
    c.bench_function("Table::get (key 10 byte, value 100 bytes)", |b| {
        let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        let key = &[0; 10];
        let value = &[0; 100];
        tbl.set(key, value).unwrap();
        b.iter(|| tbl.get(black_box(key)))
    });
    c.bench_function("Table::get (key 100 byte, value 1000 bytes)", |b| {
        let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
        let mut tbl = Table::create(file.path()).unwrap();
        let key = &[0; 100];
        let value = &[0; 1000];
        tbl.set(key, value).unwrap();
        b.iter(|| tbl.get(black_box(key)))
    });
    c.bench_function("HashMap::get (key 1 byte, value 1 bytes)", |b| {
        let mut hashmap = HashMap::with_capacity(100);
        let key = vec![0u8; 1];
        let value = vec![0u8; 1];
        hashmap.insert(key.clone(), value);
        b.iter(|| hashmap.get(black_box(&key)))
    });
    c.bench_function("HashMap::get (key 10 byte, value 100 bytes)", |b| {
        let mut hashmap = HashMap::with_capacity(100);
        let key = vec![0u8; 10];
        let value = vec![0u8; 100];
        hashmap.insert(key.clone(), value);
        b.iter(|| hashmap.get(black_box(&key)))
    });
    c.bench_function("HashMap::get (key 100 byte, value 1000 bytes)", |b| {
        let mut hashmap = HashMap::with_capacity(100);
        let key = vec![0u8; 100];
        let value = vec![0u8; 1000];
        hashmap.insert(key.clone(), value);
        b.iter(|| hashmap.get(black_box(&key)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
