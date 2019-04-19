use crate::protocol::{is_multiline_message, ProtocolHandler, ProtocolMessage, ServerInfo};
use std::sync::{Arc, RwLock};

use crate::ClientOptions;
use crate::Result;
use crossbeam_channel::{Receiver, Sender};
use nats_types::DeliveredMessage;
use std::thread;
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    str::FromStr,
};

#[derive(Clone)]
pub(crate) struct TcpClient {
    connect_urls: Arc<RwLock<Vec<ServerInfo>>>,
    delivery_sender: Sender<DeliveredMessage>,
    write_sender: Sender<Vec<u8>>,
    write_receiver: Receiver<Vec<u8>>,
    opts: ClientOptions,
    connlatch: Sender<bool>,
}

impl TcpClient {
    pub fn new(
        connect_urls: Vec<ServerInfo>,
        delivery_sender: Sender<DeliveredMessage>,
        write_sender: Sender<Vec<u8>>,
        write_receiver: Receiver<Vec<u8>>,
        opts: ClientOptions,
        connlatch: Sender<bool>,
    ) -> TcpClient {
        TcpClient {
            connect_urls: Arc::new(RwLock::new(connect_urls)),
            delivery_sender,
            write_sender,
            write_receiver,
            opts,
            connlatch,
        }
    }

    pub fn connect(&self) -> Result<()> {
        let stream_reader = {
            let urls = self.connect_urls.read().unwrap();
            Self::connect_to_host(&urls)?
        };
        let mut stream_writer = stream_reader.try_clone()?;
        let mut buf_reader = BufReader::new(stream_reader);

        let ph = ProtocolHandler::new(self.opts.clone(), self.delivery_sender.clone());
        let write_sender = self.write_sender.clone();
        let write_receiver = self.write_receiver.clone();
        let connlatch = self.connlatch.clone();
        let urls = self.connect_urls.clone();

        thread::spawn(move || {
            let mut line = String::new();

            loop {
                match buf_reader.read_line(&mut line) {
                    Ok(line_len) if line_len > 0 => {
                        if is_multiline_message(&line) {
                            let mut line2 = String::new();
                            buf_reader.read_line(&mut line2).unwrap(); // TODO: kill this unwrap
                            line.push_str(&line2);
                        }
                        let pm = ProtocolMessage::from_str(&line).unwrap(); // TODO: kill this unwrap
                        ph.handle_protocol_message(&pm, &write_sender).unwrap();
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error receiving data: {}", e);
                    }
                }
                line.clear();
            }
        });

        thread::spawn(move || {
            loop {
                let vec = write_receiver.recv().unwrap();
                match stream_writer.write_all(&vec) {
                    Ok(_) => {
                        trace!("SEND {} bytes", vec.len());
                        if starts_with(&vec, b"CONNECT") {
                            connlatch.send(true).unwrap();
                        }
                    }
                    Err(e) => error!("Failed to write buffer: {}", e), // TODO: we get this when we've been disconnected
                };
            }
        });

        Ok(())
    }

    fn connect_to_host(servers: &[ServerInfo]) -> Result<TcpStream> {
        for si in servers {
            debug!("Attempting to connect to {}:{}", si.host, si.port);
            let stream = TcpStream::connect((si.host.as_ref(), si.port));
            match stream {
                Ok(s) => return Ok(s),
                Err(e) => {
                    error!("Failed to establish TCP connection: {}", e);
                    continue;
                }
            }
        }

        Err(err!(IOError, "Failed to establish TCP connection"))
    }
}
/*
pub(crate) fn start_comms(
    host: &str,
    port: u16,
    delivery_sender: channel::Sender<DeliveredMessage>,
    write_sender: channel::Sender<Vec<u8>>,
    write_receiver: channel::Receiver<Vec<u8>>,
    opts: ClientOptions,
    s: crossbeam_channel::Sender<bool>,
) -> Result<()> {
    let stream_reader = TcpStream::connect((host.as_ref(), port))?;
    let mut stream_writer = stream_reader.try_clone()?;
    let mut buf_reader = BufReader::new(stream_reader);

    thread::spawn(move || {
        let mut line = String::new();
        let ph = ProtocolHandler::new(opts, delivery_sender);
        loop {
            match buf_reader.read_line(&mut line) {
                Ok(line_len) if line_len > 0 => {
                    if is_multiline_message(&line) {
                        let mut line2 = String::new();
                        buf_reader.read_line(&mut line2).unwrap(); // TODO: kill this unwrap
                        line.push_str(&line2);
                    }
                    let pm = ProtocolMessage::from_str(&line).unwrap(); // TODO: kill this unwrap
                    ph.handle_protocol_message(&pm, &write_sender).unwrap();
                }
                Ok(_) => {}
                Err(e) => {
                    error!("Error receiving data: {}", e);
                }
            }
            line.clear();
        }
    });

    thread::spawn(move || loop {
        let vec = write_receiver.recv().unwrap();
        match stream_writer.write_all(&vec) {
            Ok(_) => {
                trace!("SEND {} bytes", vec.len());
                if starts_with(&vec, b"CONNECT") {
                    s.send(true).unwrap();
                }
            }
            Err(e) => error!("Failed to write buffer: {}", e), // TODO: we get this when we've been disconnected
        };
    });

    Ok(())
} */

fn starts_with(haystack: &[u8], needle: &[u8]) -> bool {
    let pos = haystack
        .windows(needle.len())
        .position(|window| window == needle);
    if let Some(p) = pos {
        p == 0
    } else {
        false
    }
}
