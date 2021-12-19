#![warn(missing_docs)]
//! This crate implements a hash table that is persisted on disk as a file and accessed via mmap.
//!
//! The hash table can store keys and values as `&[u8]` of arbitrary length.
//! With the `msgpack` feature enabled, any type that can be (de-)serialized with serde/msgpack can be stored.
//!
//! The hash table consists of two parts:
//! 1) an actual hash table that stores the hash of the key and the position and size of the key/value data.
//! 2) a memory-managed data section where keys and values are stored.
//! Both parts grow and shrink automatically depending on usage.
//!
//! The used algorithms are optimized for performance so that the data storage should be faster that a regular
//! database.
//!
//! ## Simple storage
//! ```
//! use rust_persist::Table;
//!
//! let mut table = Table::create("example1.tbl").expect("Failed to create table");
//! table.set("hello".as_bytes(), "world".as_bytes()).expect("Failed to store value");
//! assert_eq!(table.get("hello".as_bytes()), Some("world".as_bytes()));
//! table.delete("hello".as_bytes()).expect("Failed to delete value");
//! ```
//!
//! ## Iterating over table values
//! ```
//! use rust_persist::Table;
//!
//! let mut table = Table::create("example2.tbl").unwrap();
//! table.set("key1".as_bytes(), "value1".as_bytes()).unwrap();
//! table.set("key2".as_bytes(), "value2".as_bytes()).unwrap();
//! for entry in table.iter() {
//!   println!("{}: {}", String::from_utf8_lossy(entry.key), String::from_utf8_lossy(entry.value));
//! }
//! ```
//!
//! ## Working with serialized data
//! ```
//! use rust_persist::Table;
//!
//! let mut table = Table::create("example3.tbl").unwrap();
//! table.set_obj("key1", vec![1,2,3]).unwrap();
//! table.set_obj("key2", (true, "string".to_string())).unwrap();
//! assert_eq!(table.get_obj("key1").unwrap(), Some(vec![1,2,3]));
//! assert_eq!(table.get_obj("key2").unwrap(), Some((true, "string".to_string())));
//! ```

use std::{hash::Hasher, io};

use index::{Hash, IndexEntry};

mod index;
mod iter;
mod memmngr;
mod mmap;
#[cfg(feature = "msgpack")]
mod msgpack;
mod resize;
mod table;
#[cfg(test)]
mod tests;

#[cfg(feature = "msgpack")]
pub use msgpack::{deserialize, serialize, TypedTable};
pub use table::{Entry, EntryMut, Table};

const INDEX_HEADER: [u8; 16] = *b"rust-persist-01\n";

const MAX_USAGE: f64 = 0.9;
const MIN_USAGE: f64 = 0.35;
const INITIAL_INDEX_CAPACITY: usize = 128;
const INITIAL_DATA_SIZE: usize = 0;

#[derive(Debug)]
/// Error type
pub enum Error {
    /// Any IO error
    Io(io::Error),
    /// The given file is not a valid table, as it has an invalid header
    WrongHeader,
    /// The table is locked by another process
    TableLocked,
    #[cfg(feature = "msgpack")]
    /// A key or value could not be deserialized
    Deserialize(rmp_serde::decode::Error),
    #[cfg(feature = "msgpack")]
    /// A key or value could not be serialized
    Serialize(rmp_serde::encode::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => {
                f.write_str("Io error")?;
                err.fmt(f)
            }
            Error::WrongHeader => f.write_str("Persistence error: File has wrong header"),
            Error::TableLocked => f.write_str("Persistence error: Table is locked"),
            Error::Deserialize(err) => {
                f.write_str("Persistence error: Failed to deserialize data:")?;
                err.fmt(f)
            }
            Error::Serialize(err) => {
                f.write_str("Persistence error: Failed to serialize data:")?;
                err.fmt(f)
            }
        }
    }
}

impl std::error::Error for Error {}
