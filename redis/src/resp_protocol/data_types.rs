use std::{
    io::{BufReader, Read},
    net::TcpStream,
};

use anyhow::{anyhow, Result};

use crate::{log, prelude::*, resp_protocol::util};

pub fn read_next_data_mandatory(reader: &mut BufReader<&TcpStream>) -> Option<RESPType> {
    return read_next_data(reader, false);
}

pub fn read_next_data_optional(reader: &mut BufReader<&TcpStream>) -> Option<RESPType> {
    return read_next_data(reader, true);
}

#[derive(Debug)]
pub enum RESPType {
    Array { size: usize },
    BulkString { size: usize },
    Error { _msg: String },
    SimpleString { value: String },
    // TODO: Integer { value: i32 },
}

pub struct ArrayStack {
    vec: Vec<usize>,
}

impl ArrayStack {
    pub fn new() -> Self {
        return ArrayStack { vec: Vec::new() };
    }

    pub fn start_new_array(&mut self, size: usize) {
        log::info(f!("Starting array of size {}", size));
        self.vec.push(size);
    }

    pub fn decrement(&mut self) -> Result<usize> {
        return match self.vec.pop() {
            Some(curr) => {
                let curr = curr - 1;
                if curr > 0 {
                    log::debug(f!("Expecting {} items", curr));
                    self.vec.push(curr);
                } else {
                    log::info(f!("End of the current array. Stack size: {}", self.vec.len()));
                }
                return Ok(curr);
            },
            None => Err(anyhow!("We expect the cmd to be inside and array")),
        };
    }

    pub fn expects_more(&self) -> bool {
        if let Some(last) = self.vec.last() {
            return *last > 0;
        }
        return false;
    }
}

fn read_next_data(reader: &mut BufReader<&TcpStream>, optional: bool) -> Option<RESPType> {
    for byte in reader.bytes() {
        if let Err(error) = byte {
            return if optional {
                None
            } else {
                Some(RESPType::Error {
                    _msg: f!("Could not read next data type symbol: {}", error),
                })
            };
        }

        return match parse_data(byte.unwrap(), reader) {
            Ok(parsed) => Some(parsed),
            Err(e) => Some(RESPType::Error {
                _msg: e.to_string(),
            }),
        };
    }
    return None;
}

fn parse_data(data_type_char: u8, reader: &mut BufReader<&TcpStream>) -> Result<RESPType> {
    // TODO:
    // b'-' => self.parse_error(&self.buffer[1..]),
    // b':' => self.parse_integer(&self.buffer[1..]),

    log::debug(f!("Got data_type_char {}", (data_type_char as char)));
    return match data_type_char {
        b'*' => parse_array(reader),
        b'+' => parse_simple_string(reader),
        b'$' => parse_bulk_string(reader),
        _ => Err(anyhow!(
            "Unsupported data type {}",
            char::from(data_type_char)
        )),
    };
}

fn parse_simple_string(
    reader: &mut BufReader<&TcpStream>,
) -> std::result::Result<RESPType, anyhow::Error> {
    log::debug("Parsing RESP Simple String!");
    let value = util::read_until_line_break(reader, 1024)?;
    return Ok(RESPType::SimpleString { value: String::from_utf8(value)? });
}

fn parse_bulk_string<'a>(reader: &mut BufReader<&TcpStream>) -> Result<RESPType> {
    log::debug("Parsing RESP BulkString!");
    let bulk_string_size = util::read_size(reader)?;
    log::debug(f!("Parsed a RESP BulkString of size {}", bulk_string_size));
    return Ok(RESPType::BulkString {
        size: bulk_string_size,
    });
}

fn parse_array(reader: &mut BufReader<&TcpStream>) -> Result<RESPType> {
    log::debug("Parsing RESP Array!");
    let array_size = util::read_size(reader)?;
    log::debug(f!("Parsed a RESP Array of size {}", array_size));
    return Ok(RESPType::Array { size: array_size });
}
