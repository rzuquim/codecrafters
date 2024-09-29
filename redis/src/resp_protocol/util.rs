use std::{
    io::{BufReader, Read},
    net::TcpStream,
};

use anyhow::{anyhow, Context, Result};

use crate::{log, prelude::*};

use super::data_types::{self, RESPType};

pub fn read_size(reader: &mut BufReader<&TcpStream>) -> Result<usize> {
    let expected_size = read_until_line_break(reader, 10)?;
    let size_str = std::str::from_utf8(&expected_size)
        .context(f!("[ERR] Expected UTF8! Got: {:?}!", expected_size))?;
    let size = size_str
        .parse::<usize>()
        .context(f!("[ERR] Could not parse {} into uint size!", size_str))?;
    return Ok(size);
}

pub fn consume_line_break(reader: &mut BufReader<&TcpStream>) -> Result<()> {
    let mut line_break = [0; 2];
    reader.read_exact(&mut line_break)?;
    if !(&line_break == b"\r\n") {
        return match std::str::from_utf8(&line_break) {
            Ok(content) => Err(anyhow!("Expected line break after a cmd! Got {}", content)),
            Err(_) => Err(anyhow!("Expected line break after a cmd!")),
        };
    }
    return Ok(());
}

pub fn read_until_line_break(
    reader: &mut BufReader<&TcpStream>,
    max_read: usize,
) -> Result<Vec<u8>> {
    let mut last_byte = b'0';
    let mut count = 0_usize;
    let mut data: Vec<u8> = Vec::with_capacity(max_read);

    for b in reader.bytes() {
        count += 1;
        let curr_byte = b?;
        if last_byte == b'\r' && curr_byte == b'\n' {
            return Ok(data);
        }
        last_byte = curr_byte;

        if last_byte != b'\r' && curr_byte != b'\n' {
            data.push(curr_byte);
        }

        if max_read > 0 && count > max_read {
            let err_msg = f!(
                "[ERR] Could not find line break, not even after {} chars read!",
                max_read
            );
            return Err(anyhow!(err_msg));
        }
    }

    return Err(anyhow!("Missing EOL!"));
}

pub fn receive_response(reader: &mut BufReader<&TcpStream>) -> Result<String> {
    let next_data = data_types::read_next_data_mandatory(reader);
    if next_data.is_none() {
        return Err(anyhow!("Expected bulk string as SET key, got nothing."));
    }

    match next_data.unwrap() {
        RESPType::SimpleString { value } => {
            return Ok(value);
        }
        _ => {
            return Err(anyhow!("Expected simple string as response"));
        }
    }
}

pub fn assert_response(reader: &mut BufReader<&TcpStream>, expected: &str) -> Result<()> {
    let response = receive_response(reader)?;
    if response == expected {
        log::debug(f!("Reponse is as expected {}", response));
        return Ok(());
    } else {
        return Err(anyhow!(
            "Protocol expects a {} as response and got {}.",
            expected,
            response
        ));
    }
}
