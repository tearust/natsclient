use crate::protocol::is_multiline_message;
use crate::protocol::{ProtocolHandler, ProtocolMessage};

use crate::ClientOptions;
use crate::Result;
use crossbeam_channel as channel;
use nats_types::DeliveredMessage;
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    str::FromStr,
    thread,
};

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
            Err(e) => error!("Failed to write buffer: {}", e),
        };
    });

    Ok(())
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
