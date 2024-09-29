use std::{
    io::{BufWriter, Write},
    net::TcpStream,
};

use anyhow::Result;

use crate::log;

pub fn ping(writer: &mut BufWriter<&TcpStream>) -> Result<()> {
    log::debug("got PING wrote PONG in response");

    writer.write(b"+PONG\r\n")?;
    writer.flush()?;

    return Ok(());
}
