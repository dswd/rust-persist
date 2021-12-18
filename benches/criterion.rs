use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rust_persist::Table;

pub fn criterion_benchmark(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("Small data (key: 1 byte, value: 1 byte)");
        group.throughput(Throughput::Bytes(1));
        group.bench_function("Table::set", |b| {
            let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
            let mut tbl = Table::create(file.path()).unwrap();
            let key = &[0];
            let value = &[0];
            b.iter(|| tbl.set(black_box(key), black_box(value)).is_ok())
        });
        group.bench_function("HashMap::insert", |b| {
            let mut hashmap = HashMap::with_capacity(100);
            let key = vec![0u8; 1];
            let value = vec![0u8; 1];
            b.iter(|| hashmap.insert(black_box(key.clone()), black_box(value.clone())))
        });
        group.bench_function("Table::get", |b| {
            let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
            let mut tbl = Table::create(file.path()).unwrap();
            let key = &[0];
            let value = &[0];
            tbl.set(key, value).unwrap();
            b.iter(|| tbl.get(black_box(key)))
        });
        group.bench_function("HashMap::get", |b| {
            let mut hashmap = HashMap::with_capacity(100);
            let key = vec![0u8; 1];
            let value = vec![0u8; 1];
            hashmap.insert(key.clone(), value);
            b.iter(|| hashmap.get(black_box(&key)))
        });
    }
    {
        let mut group = c.benchmark_group("Medium data (key: 10 bytes, value: 100 bytes)");
        group.throughput(Throughput::Bytes(110));
        group.bench_function("Table::set", |b| {
            let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
            let mut tbl = Table::create(file.path()).unwrap();
            let key = &[0u8; 10];
            let value = &[0u8; 100];
            b.iter(|| tbl.set(black_box(key), black_box(value)).is_ok())
        });
        group.bench_function("HashMap::insert", |b| {
            let mut hashmap = HashMap::with_capacity(100);
            let key = vec![0u8; 10];
            let value = vec![0u8; 100];
            b.iter(|| hashmap.insert(black_box(key.clone()), black_box(value.clone())))
        });
        group.bench_function("Table::get", |b| {
            let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
            let mut tbl = Table::create(file.path()).unwrap();
            let key = &[0; 10];
            let value = &[0; 100];
            tbl.set(key, value).unwrap();
            b.iter(|| tbl.get(black_box(key)))
        });
        group.bench_function("HashMap::get", |b| {
            let mut hashmap = HashMap::with_capacity(100);
            let key = vec![0u8; 10];
            let value = vec![0u8; 100];
            hashmap.insert(key.clone(), value);
            b.iter(|| hashmap.get(black_box(&key)))
        });
    }
    {
        let mut group = c.benchmark_group("Large data (key: 100 bytes, value: 1000 bytes)");
        group.throughput(Throughput::Bytes(1100));
        group.bench_function("Table::set", |b| {
            let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
            let mut tbl = Table::create(file.path()).unwrap();
            let key = &[0u8; 100];
            let value = &[0u8; 1000];
            b.iter(|| tbl.set(black_box(key), black_box(value)).is_ok())
        });
        group.bench_function("HashMap::insert", |b| {
            let mut hashmap = HashMap::with_capacity(100);
            let key = vec![0u8; 100];
            let value = vec![0u8; 1000];
            b.iter(|| hashmap.insert(black_box(key.clone()), black_box(value.clone())))
        });
        group.bench_function("Table::get", |b| {
            let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
            let mut tbl = Table::create(file.path()).unwrap();
            let key = &[0; 100];
            let value = &[0; 1000];
            tbl.set(key, value).unwrap();
            b.iter(|| tbl.get(black_box(key)))
        });
        group.bench_function("HashMap::get", |b| {
            let mut hashmap = HashMap::with_capacity(100);
            let key = vec![0u8; 100];
            let value = vec![0u8; 1000];
            hashmap.insert(key.clone(), value);
            b.iter(|| hashmap.get(black_box(&key)))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
