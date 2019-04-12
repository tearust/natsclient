use crate::protocol::is_multiline_message;
use crate::protocol::{generate_connect_command, ProtocolHandler, ProtocolMessage};

use crate::ClientOptions;
use crate::Result;
use crossbeam::sync::WaitGroup;
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
    write_sender: channel::Sender<ProtocolMessage>,
    write_receiver: channel::Receiver<ProtocolMessage>,
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
                    println!("ERR: {}", e);
                }
            }
            line.clear();
        }
    });

    thread::spawn(move || loop {
        let pmsg = write_receiver.recv().unwrap();
        println!("SENDING: '{}'", pmsg.to_string());
        match stream_writer.write_all(pmsg.to_string().as_bytes()) {
            Ok(_) => {
                if let ProtocolMessage::Connect(_) = pmsg {
                    s.send(true);
                }
            }
            Err(e) => println!("failed to write buffer: {}", e),
        };
    });

    Ok(())
}

/*


        println!("RECEIVED: {}", line);
        if line.starts_with("INFO") {
            let info = ProtocolMessage::from_str(line.as_ref()).unwrap();
            let pm = generate_connect_command(&info, &auth);
            write_sender.send(pm).unwrap(); // TODO: kill unwrap
        }
    }
    Ok(_) => {}
    Err(e) => {
        println!("ERR: {}", e);
    }
}
line.clear(); */
