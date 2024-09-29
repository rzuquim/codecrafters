#![deny(clippy::implicit_return)]
#![allow(clippy::needless_return)]

mod log;
mod persistence;
mod prelude;
mod resp_protocol;

use core::panic;
use std::{
    env,
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::Result;
use persistence::{in_mem::InMemStore, Store};
use resp_protocol::data_types::ArrayStack;

use crate::prelude::*;
use crate::resp_protocol::data_types::RESPType;
use crate::resp_protocol::{cmds, data_types};

fn main() -> Result<()> {
    let config = Arc::new(parse_args());
    let address = f!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(address)?;

    match &config.role {
        ServerRole::Main { id } => start_as_main(&id)?,
        ServerRole::Replica { main_addr } => start_as_replica(&main_addr, config.port)?,
    }

    let store = InMemStore::new();
    println!("[INFO] Listening on port {}", config.port);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut store_clone = store.clone();
                let config = Arc::clone(&config);

                thread::spawn(move || {
                    handle_client(stream, &config, &mut store_clone);
                });
            }
            Err(e) => {
                println!("[FATAL]: {}", e);
            }
        };
    }

    return Ok(());
}

fn start_as_replica(main_addr: &str, port: u16) -> Result<()> {
    let parts = main_addr.split_whitespace().collect::<Vec<&str>>();

    if parts.len() != 2 {
        panic!(
            "Error starting replica! Invalid main node address: {}",
            main_addr
        );
    }

    let stream = TcpStream::connect(f!("{}:{}", parts[0], parts[1]))?;
    stream.set_read_timeout(Some(Duration::new(5, 0)))?;
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    log::debug("Sending PING to main node.");
    writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
    writer.flush()?;

    log::debug("Waiting for PONG");
    resp_protocol::util::assert_response(&mut reader, "PONG")?;

    log::debug("Sending port info to main node");
    writer.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n")?;
    writer.write_all(f!("{}\r\n", port).as_bytes())?;
    writer.flush()?;

    log::debug("Waiting for OK");
    resp_protocol::util::assert_response(&mut reader, "OK")?;

    log::debug("Sending capabilities to main node");
    writer.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")?;
    writer.flush()?;
    log::debug("Waiting for OK");
    resp_protocol::util::assert_response(&mut reader, "OK")?;

    log::debug("Sending PSYNC");
    writer.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")?;
    writer.flush()?;
    log::debug("Waiting for replica id");
    let response = resp_protocol::util::receive_response(&mut reader)?;
    log::debug(f!("Got response {}", response));

    return Ok(());
}

fn start_as_main(_id: &str) -> Result<()> {
    return Ok(());
}

fn handle_client<T: Store>(stream: TcpStream, config: &Arc<Config>, store: &mut T) {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut array_stack = ArrayStack::new();

    loop {
        log::info("Searching for new command");
        let next_data = data_types::read_next_data_optional(&mut reader);
        if next_data.is_none() {
            log::info("Reached end of stream.");
            return;
        }

        let data = next_data.unwrap();

        match data {
            RESPType::Array { size } => {
                array_stack.start_new_array(size);
            }
            RESPType::BulkString { size } => {
                match cmds::parse(size, &mut reader, &mut array_stack) {
                    Ok(cmd) => {
                        match cmd.execute(&mut reader, &mut writer, &mut array_stack, store, config)
                        {
                            Ok(_) => log::debug(f!("Cmd {:?} ran successfully", cmd)),
                            Err(e) => {
                                log::error(f!("Unexpected error executing cmd {:?}: {}", cmd, e))
                            }
                        }
                    }
                    Err(e) => {
                        log::error(f!("Unsupported cmd: {}", e));
                        // TODO: inform unsupported cmd on response writer!
                    }
                }
            }
            _ => {
                log::error(f!(
                    "[ERR] Expecting a array or a bulk string with a cmd {:?}",
                    data
                ));
                // TODO: inform invalid msg on response writer!
                return;
            }
        }
    }
}

// TODO: use clap
fn parse_args() -> Config {
    let args = env::args().collect::<Vec<String>>();
    let mut cfg = Config::default();
    let mut capture = "";

    for arg in args.iter().skip(1) {
        if arg.starts_with("--") {
            capture = arg;
            continue;
        }

        if capture == "--port" {
            cfg.port = arg.parse::<u16>().expect("Valid port");
        } else if capture == "--replicaof" {
            cfg.role = ServerRole::Replica {
                main_addr: arg.clone(),
            };
        } else {
            panic!("Usage: cargo run -- --port <PORT>");
        }
    }

    return cfg;
}

struct Config {
    port: u16,
    role: ServerRole,
}

impl Config {
    fn default() -> Config {
        return Config {
            port: 6379,
            // TODO: generate random id
            role: ServerRole::Main {
                id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            },
        };
    }
}

pub enum ServerRole {
    Main { id: String },
    Replica { main_addr: String },
}
