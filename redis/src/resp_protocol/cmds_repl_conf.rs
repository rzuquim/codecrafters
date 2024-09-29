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

pub fn repl_conf(
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    array_stack: &mut ArrayStack,
    _config: &Config,
) -> Result<()> {
    while array_stack.expects_more() {
        let next_data = data_types::read_next_data_mandatory(reader);

        if next_data.is_none() {
            return Err(anyhow!("Expected bulk string to as repl conf param"));
        }

        match next_data.unwrap() {
            data_types::RESPType::BulkString { size } => {
                log::debug(f!("Reading data to SET, of size {}", size));

                let mut value_bytes = vec![0; size];
                reader.read_exact(&mut value_bytes)?;
                let value = String::from_utf8(value_bytes)?;
                log::info(f!("Read REPLCONF param {}", value));
                util::consume_line_break(reader)?;
            }
            _ => return Err(anyhow!("[ERR] Expected bulk string as REPLCONF param")),
        }
        _ = array_stack.decrement();
    }
    writer.write(b"+OK\r\n")?;
    writer.flush()?;

    return Ok(());
}
