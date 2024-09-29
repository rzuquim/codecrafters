use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
};

use anyhow::{anyhow, Context, Ok, Result};

use crate::{log, persistence::Store, prelude::*};

use super::{data_types, util};

pub fn set<T: Store>(
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    array_stack: &mut data_types::ArrayStack,
    store: &mut T,
) -> Result<()> {
    let key = read_key(reader)?;
    array_stack.decrement()?;
    let value = read_value(reader)?;
    array_stack.decrement()?;

    let expiration = if array_stack.expects_more() { read_expiration_multiplier(reader)? } else { None };
    let mut expiration_value: Option<u32> = None;

    if expiration.is_some() {
        expiration_value =
            Some(read_expiration_value(reader).context("Must set expiration value!")?);
        log::info(f!("Entry expires in {:?} * {:?} ms", expiration, expiration_value));
    }

    if expiration.is_some() && expiration_value.is_some() {
        store.set_expiring(key, value, expiration_value.unwrap() * expiration.unwrap());
    } else {
        store.set(key, value);
    }

    writer.write(b"+OK\r\n")?;
    writer.flush()?;

    return Ok(());
}

fn read_key(reader: &mut BufReader<&TcpStream>) -> Result<String> {
    let next_data = data_types::read_next_data_mandatory(reader);

    if next_data.is_none() {
        return Err(anyhow!("Expected bulk string as SET key, got nothing."));
    }

    match next_data.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Reading key to SET, of size {}", size));

            let mut key_bytes = vec![0; size];
            reader.read_exact(&mut key_bytes)?;
            let key = String::from_utf8(key_bytes)?;
            log::info(f!("Read key to SET {}", key));
            util::consume_line_break(reader)?;
            return Ok(key);
        }
        _ => return Err(anyhow!("[ERR] The key to be SET must be bulk string!")),
    }
}

fn read_value(reader: &mut BufReader<&TcpStream>) -> Result<String> {
    let next_data = data_types::read_next_data_mandatory(reader);

    if next_data.is_none() {
        return Err(anyhow!("Expected bulk string to echo, got nothing."));
    }

    match next_data.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Reading data to SET, of size {}", size));

            let mut value_bytes = vec![0; size];
            reader.read_exact(&mut value_bytes)?;
            let value = String::from_utf8(value_bytes)?;
            log::info(f!("Read value to SET {}", value));
            util::consume_line_break(reader)?;
            return Ok(value);
        }
        _ => return Err(anyhow!("[ERR] The value to be SET must be bulk string!")),
    }
}

fn read_expiration_multiplier(reader: &mut BufReader<&TcpStream>) -> Result<Option<u32>> {
    let maybe_px = data_types::read_next_data_optional(reader);

    if maybe_px.is_none() {
        log::debug("No expiration in SET!");
        return Ok(None);
    }

    match maybe_px.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Reading expiration type arg of size {}", size));

            let mut px_arg_bytes = vec![0; size];
            reader.read_exact(&mut px_arg_bytes)?;
            util::consume_line_break(reader)?;

            let px_arg = String::from_utf8(px_arg_bytes)?;
            let lowercase_px_arg = px_arg.to_lowercase();

            match lowercase_px_arg.as_str() {
                "px" => Ok(Some(1)),
                "ex" => Ok(Some(1000)),
                _ => return Err(anyhow!("[ERR] Unsupported SET argument {}!", px_arg)),
            }
        }
        _ => return Err(anyhow!("[ERR] The SET arg must be a bulk string!")),
    }
}

fn read_expiration_value(reader: &mut BufReader<&TcpStream>) -> Result<u32> {
    let expiration = data_types::read_next_data_mandatory(reader);

    match expiration.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Reading expiration arg of size {}", size));

            let mut expiration_bytes = vec![0; size];
            reader.read_exact(&mut expiration_bytes)?;
            util::consume_line_break(reader)?;

            let value_text = String::from_utf8(expiration_bytes)?;
            let parsed_as_int = value_text.parse::<u32>()?;
            return Ok(parsed_as_int);
        }
        _ => return Err(anyhow!("[ERR] The SET arg must be a bulk string!")),
    }
}
