use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
};

use anyhow::{anyhow, Ok, Result};

use crate::{
    log,
    prelude::*,
    resp_protocol::data_types,
};

pub fn echo(
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    array_stack: &mut data_types::ArrayStack,
) -> Result<()> {
    let next_data = data_types::read_next_data_mandatory(reader);
    array_stack.decrement()?;

    if next_data.is_none() {
        return Err(anyhow!("Expected bulk string as SET key, got nothing."));
    }

    match next_data.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Echoing string of size {}", size));
            // TODO: we can also validate the actual content against the declared size to detect errors
            // TODO: according to the documentation the max size of a bulk string is 512MB we
            //       should enforce it here.
            writer.write(f!("${}\r\n", size).as_bytes())?;

            let mut last_byte = b'0';
            let mut write_count = 0_usize;

            for b in reader.bytes() {
                let curr_byte = b?;
                if last_byte == b'\r' && curr_byte == b'\n' {
                    break;
                }

                last_byte = curr_byte;

                if last_byte != b'\r' && curr_byte != b'\n' {
                    write_count += 1;
                    writer.write(&[curr_byte])?;
                }
            }

            if write_count != size {
                // NOTE: not flushing invalid data (less or more than declared size)
                return Err(anyhow!(
                    "[ERR] Declared {} bytes to ECHO but received {}.",
                    size,
                    write_count
                ));
            }

            writer.write(b"\r\n")?;
            writer.flush()?;
            return Ok(());
        }
        _ => {
            return Err(anyhow!(
                "[ERR] An echo cmd must be followed by a bulk string!"
            ))
        }
    }
}
