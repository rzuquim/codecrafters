use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream, sync::Arc,
};

use anyhow::{anyhow, Ok, Result};

use crate::{log, prelude::*, resp_protocol::data_types, Config, ServerRole};

pub fn info(
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    array_stack: &mut data_types::ArrayStack,
    config: &Arc<Config>,
) -> Result<()> {
    // TODO: multiple section selectors: INFO [section [section ...]]
    let info_section = if array_stack.expects_more() {
        Some(read_info_section(reader)?)
    } else {
        None
    };

    if info_section.is_none() {
        todo!("Support info without section selector");
    }

    if let Some(info_section) = info_section {
        if info_section != "replication" {
            todo!("Support other info sections");
        }
    }

    match &config.role {
        ServerRole::Main { id } => write_main_data(writer, &id)?,
        ServerRole::Replica { main_addr: _ } => writer.write(b"$10\r\nrole:slave\r\n")?,
    };

    writer.flush()?;

    // BUG: only if expects_more()
    // util::consume_line_break(reader)?;
    return Ok(());
}

fn write_main_data(writer: &mut BufWriter<&TcpStream>, id: &String) -> Result<usize> {
    let mut bytes = 0;
    let response = f!("role:master\r\nmaster_replid:{id}\r\nmaster_repl_offset:0");
    bytes += writer.write(f!("$89\r\n{response}\r\n").as_bytes())?;
    return Ok(bytes);
}

fn read_info_section(reader: &mut BufReader<&TcpStream>) -> Result<String> {
    let next_data = data_types::read_next_data_mandatory(reader);

    if next_data.is_none() {
        return Err(anyhow!(
            "Expected bulk string as INFO section, got nothing."
        ));
    }

    match next_data.unwrap() {
        data_types::RESPType::BulkString { size } => {
            log::debug(f!("Reading INFO section, of size {}", size));

            let mut key_bytes = vec![0; size];
            reader.read_exact(&mut key_bytes)?;
            let info_section = String::from_utf8(key_bytes)?;
            log::info(f!("Read INFO section {}", info_section));
            return Ok(info_section);
        }
        _ => return Err(anyhow!("[ERR] The section INFO must be a bulk string!")),
    }
}
