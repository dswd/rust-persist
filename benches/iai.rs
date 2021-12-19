use iai::{main, black_box};
use rust_persist::Table;

fn init() -> Table {
    let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
    Table::create(file.path()).unwrap()
}

fn set() -> Table {
    let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let key = &[0u8; 10];
    let value = &[0u8; 100];
    for _ in 0..1000 {
        tbl.set(black_box(key), black_box(value)).unwrap();
    }
    tbl
}

fn get() -> Table {
    let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let key = &[0u8; 10];
    let value = &[0u8; 100];
    tbl.set(key, value).unwrap();
    for _ in 0..1000 {
        tbl.get(black_box(key)).unwrap();
    }
    tbl
}

main!(init, set, get);
