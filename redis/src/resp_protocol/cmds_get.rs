use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
};

use anyhow::{anyhow, Ok, Result};

use crate::{
    log,
    persistence::Store,
    prelude::*,
};

use super::{data_types, util};

pub fn get<T: Store>(
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    array_stack: &mut data_types::ArrayStack,
    store: &mut T,
) -> Result<()> {
    let key = read_key(reader)?;
    log::debug(f!("Fetching key {}", &key));
    let maybe_value = store.get(&key);

    match maybe_value {
        Some(value) => {
            array_stack.decrement()?;
            write_value(writer, value.as_str())?;
        }
        None => {
            writer.write(b"$-1\r\n")?;
        }
    }

    writer.flush()?;

    return Ok(());
}

fn read_key(reader: &mut BufReader<&TcpStream>) -> Result<String> {
    let next_data = data_types::read_next_data_mandatory(reader);

    if next_data.is_none() {
        return Err(anyhow!("Expected bulk string as GET key, got nothing."));
    }

    match next_data.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Reading key to GET, of size {}", size));

            let mut key_bytes = vec![0; size];
            reader.read_exact(&mut key_bytes)?;
            let key = String::from_utf8(key_bytes)?;
            util::consume_line_break(reader)?;
            return Ok(key);
        }
        _ => return Err(anyhow!("[ERR] The key to be GET must be bulk string!")),
    }
}

fn write_value(writer: &mut BufWriter<&TcpStream>, value: &str) -> Result<()> {
    writer.write(f!("${}\r\n{}\r\n", value.len(), value).as_bytes())?;
    writer.flush()?;
    return Ok(());
}
