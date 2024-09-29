use std::{
    io::{BufReader, BufWriter, Read},
    net::TcpStream,
    sync::Arc,
};

use anyhow::{anyhow, Context, Ok, Result};

use crate::{log, persistence::Store, prelude::*, resp_protocol::util, Config};

use super::{echo, get, info, ping, psync, repl_conf, set};

use super::data_types::ArrayStack;

#[derive(Debug)]
pub enum RESPCmd {
    PING,
    ECHO,
    SET,
    GET,
    INFO,
    REPLCONF,
    PSYNC,
}

pub fn parse(
    bulk_string_size: usize,
    reader: &mut BufReader<&TcpStream>,
    array_stack: &mut ArrayStack,
) -> Result<RESPCmd> {
    log::debug(f!("Parsing cmd of size {}", bulk_string_size));

    let mut buffer = vec![0; bulk_string_size];
    reader.read_exact(&mut buffer)?;

    util::consume_line_break(reader)?;

    let cmd_id = std::str::from_utf8(&buffer).context(f!("Expected UTF8! Got: {:?}!", buffer))?;

    let case_insensitive_cmd_id = cmd_id.to_uppercase();
    let cmd_id = case_insensitive_cmd_id.as_str();

    log::info(f!("Received cmd {}", cmd_id));
    array_stack.decrement()?;

    return match cmd_id {
        "PING" => Ok(RESPCmd::PING),
        "ECHO" => Ok(RESPCmd::ECHO),
        "SET" => Ok(RESPCmd::SET),
        "GET" => Ok(RESPCmd::GET),
        "INFO" => Ok(RESPCmd::INFO),
        "REPLCONF" => Ok(RESPCmd::REPLCONF),
        "PSYNC" => Ok(RESPCmd::PSYNC),
        _ => Err(anyhow!("Unsupported cmd {}", cmd_id)),
    };
}

impl RESPCmd {
    pub fn execute<T: Store>(
        &self,
        reader: &mut BufReader<&TcpStream>,
        writer: &mut BufWriter<&TcpStream>,
        array_stack: &mut ArrayStack,
        store: &mut T,
        config: &Arc<Config>,
    ) -> Result<()> {
        log::debug(f!("Running cmd {:?}", &self));
        return match &self {
            RESPCmd::PING => ping(writer),
            RESPCmd::ECHO => echo(reader, writer, array_stack),
            RESPCmd::SET => set(reader, writer, array_stack, store),
            RESPCmd::GET => get(reader, writer, array_stack, store),
            RESPCmd::INFO => info(reader, writer, array_stack, config),
            RESPCmd::REPLCONF => repl_conf(reader, writer, array_stack, config),
            RESPCmd::PSYNC => psync(reader, writer, array_stack, config),
        };
    }
}
