use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
};

use crate::prelude::*;

use anyhow::{anyhow, Result};

use crate::{log, Config};

use super::{
    data_types::{self, ArrayStack},
    util,
};

pub fn psync(
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    array_stack: &mut ArrayStack,
    _config: &Config,
) -> Result<()> {
    while array_stack.expects_more() {
        let next_data = data_types::read_next_data_mandatory(reader);

        if next_data.is_none() {
            return Err(anyhow!("Expected bulk string as PSYNC param"));
        }

        match next_data.unwrap() {
            data_types::RESPType::BulkString { size } => {
                log::debug(f!("Reading PSYNC param of size {}", size));

                let mut value_bytes = vec![0; size];
                reader.read_exact(&mut value_bytes)?;
                let value = String::from_utf8(value_bytes)?;
                log::info(f!("Read PSYNC param {}", value));
                util::consume_line_break(reader)?;
            }
            _ => return Err(anyhow!("[ERR] Expected bulk string as REPLCONF param")),
        }
        _ = array_stack.decrement();
    }
    writer.write(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")?;
    let empty_rdb_file = hex_to_bytes("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
    writer.write(b"$88\r\n")?;
    writer.write(&empty_rdb_file)?;
    writer.flush()?;

    return Ok(());
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(anyhow!("Hex string has an odd length"));
    }

    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>();

    match bytes {
        Ok(vec) => Ok(vec),
        Err(_) => Err(anyhow!("Failed to parse hex string")),
    }
}
