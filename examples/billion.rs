use rust_persist::*;


pub fn main() {
    let mut table = Table::create("/dev/shm/billion.tbl").unwrap();
    for _ in 0..50_000 {
        for i in 0u16..1000 {
            table.set(&i.to_be_bytes(), &[]).unwrap();
        }
        for _ in 1..20 {
            for i in 0u16..1000 {
                table.get(&i.to_be_bytes()).unwrap();
            }
        }
    }
}