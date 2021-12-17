use std::collections::HashMap;

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

use crate::Database;

type Rand = ChaCha8Rng;

fn seeded_rng(s: u64) -> Rand {
    let mut seed: <Rand as SeedableRng>::Seed = Default::default();
    seed[0..8].copy_from_slice(&s.to_ne_bytes());
    Rand::from_seed(seed)
}

fn random_data(rand: &mut Rand, max_size: usize) -> Vec<u8> {
    let size = rand.gen_range(1..max_size);
    let mut data = vec![0; size];
    rand.fill_bytes(&mut data);
    data
}

fn test_one_seed(seed: u64) {
    let mut rand = seeded_rng(seed);
    let mut data = HashMap::new();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::create(file.path()).unwrap();
    let count = rand.gen_range(100..1000);
    for _ in 0..count / 2 {
        let key = random_data(&mut rand, 100);
        let value = random_data(&mut rand, 1000);
        db.set(&key, &value).unwrap();
        assert!(db.is_valid());
        data.insert(key, value);
    }
    db.close();
    let mut db = Database::open(file.path()).unwrap();
    assert!(db.is_valid());
    for _ in count / 2..count {
        let key = random_data(&mut rand, 100);
        let value = random_data(&mut rand, 1000);
        db.set(&key, &value).unwrap();
        assert!(db.is_valid());
        data.insert(key, value);
    }
    db.close();
    let mut db = Database::open(file.path()).unwrap();
    assert!(db.is_valid());
    for (key, value) in data {
        let stored = db.get(&key);
        assert!(stored.is_some());
        assert_eq!(stored.unwrap(), &value);
        let removed = db.delete(&key).unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), &value);
        let stored = db.get(&key);
        assert!(stored.is_none());
        assert!(db.is_valid());
    }
    assert!(db.is_empty())
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
