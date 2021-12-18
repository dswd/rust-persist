use iai::main;
use rust_persist::Table;

fn init() -> Table {
    let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
    Table::create(file.path()).unwrap()
}

fn set() {
    let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let key = &[0u8; 10];
    let value = &[0u8; 100];
    for _ in 0..1000 {
        tbl.set(key, value).unwrap();
    }
}

fn get() {
    let file = tempfile::NamedTempFile::new_in("/dev/shm").unwrap();
    let mut tbl = Table::create(file.path()).unwrap();
    let key = &[0u8; 10];
    let value = &[0u8; 100];
    tbl.set(key, value).unwrap();
    for _ in 0..1000 {
        tbl.get(key).unwrap();
    }
}

main!(init, set, get);
