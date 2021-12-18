use std::{
    env::args,
    io::{stdin, stdout, Read, Write},
    path::PathBuf,
};

use rust_persist::{Error, Table};

fn usage() {
    eprintln!("Usage: textdb PATH CMD [KEY]");
    eprintln!();
    eprintln!("Commands:");
    eprintln!(" - init:   Initialize new table");
    eprintln!(" - clear:  Clear table");
    eprintln!(" - set:    Set value for KEY from stdin");
    eprintln!(" - get:    Get value for KEY and print to stdout");
    eprintln!(" - delete: Delete KEY from table");
}

fn cmd_get(table: &mut Table, key: &str) -> Result<(), Error> {
    if let Some(value) = table.get(key.as_bytes()) {
        stdout().write_all(value).map_err(Error::Io)?;
    } else {
        eprintln!("Key '{}' not found", key);
    }
    Ok(())
}

fn cmd_set(table: &mut Table, key: &str) -> Result<(), Error> {
    let mut input = vec![];
    stdin().read_to_end(&mut input).map_err(Error::Io)?;
    table.set(key.as_bytes(), &input)?;
    Ok(())
}

fn cmd_delete(table: &mut Table, key: &str) -> Result<(), Error> {
    if table.delete(key.as_bytes())?.is_none() {
        eprintln!("Key '{}' not found", key);
    }
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn cmd_list(table: &mut Table) -> Result<(), Error> {
    if table.is_empty() {
        eprintln!("Table is empty");
    }
    for entry in table.iter() {
        println!("{}", String::from_utf8_lossy(entry.key));
    }
    Ok(())
}

fn cmd_clear(table: &mut Table) -> Result<(), Error> {
    table.clear()
}

pub fn main() -> Result<(), Error> {
    let mut args = args();
    if args.len() < 3 {
        usage();
        return Ok(());
    }
    args.next().unwrap();
    let table_path = PathBuf::from(args.next().unwrap());
    let cmd = args.next().unwrap();
    if cmd == "init" {
        Table::create(table_path)?;
        return Ok(());
    }
    let mut table = Table::open(table_path)?;
    match &cmd as &str {
        "get" | "set" | "delete" => {
            if let Some(key) = args.next() {
                match &cmd as &str {
                    "get" => cmd_get(&mut table, &key),
                    "set" => cmd_set(&mut table, &key),
                    "delete" => cmd_delete(&mut table, &key),
                    _ => unreachable!(),
                }
            } else {
                usage();
                Ok(())
            }
        }
        "clear" => cmd_clear(&mut table),
        "list" => cmd_list(&mut table),
        _ => {
            usage();
            Ok(())
        }
    }
}
