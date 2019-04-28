use crate::protocol::{ProtocolHandler, ProtocolMessage, ServerInfo};
use crate::ClientOptions;
use crate::Result;
use crossbeam_channel::{Receiver, Sender};
use nats_types::DeliveredMessage;
use std::io::Read;
use std::sync::{Arc, RwLock};
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

        thread::spawn(move || {
            let mut line = String::new();

            loop {
                match buf_reader.read_line(&mut line) {
                    Ok(line_len) if line_len > 0 => {
                        let pm = if line.starts_with("MSG") {
                            let msgheader = nats_types::parse_msg_header(&line).unwrap(); // TODO kill unwrap
                            let mut buffer = vec![0; msgheader.message_len];
                            buf_reader.read_exact(&mut buffer).unwrap(); // TODO kill unwrap

                            buf_reader.read_line(&mut line).unwrap(); // purge the line feed

                            ProtocolMessage::Message(DeliveredMessage {
                                reply_to: msgheader.reply_to,
                                payload_size: msgheader.message_len,
                                payload: buffer,
                                subject: msgheader.subject,
                                subscription_id: msgheader.sid,
                            })
                        } else {
                            ProtocolMessage::from_str(&line).unwrap() // TODO: kill this unwrap
                        };

                        line.clear();
                        ph.handle_protocol_message(&pm, &write_sender).unwrap();
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error receiving data: {}", e);
                    }
                }
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
