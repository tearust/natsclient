use crate::ClientOptions;
use crate::{AuthenticationStyle, Result};
use crossbeam_channel as channel;
use nats_types::DeliveredMessage;
use nkeys::KeyPair;
use rand;
use rand::{seq::SliceRandom, thread_rng};
use std::sync::mpsc::Sender;
use url::Url;

pub use nats_types::{ConnectionInformation, ProtocolMessage};

#[derive(Debug, Clone)]
pub(crate) struct ServerInfo {
    pub host: String,
    pub port: u16,
}

const URI_SCHEME: &str = "nats";
const DEFAULT_NAME: &str = "#natsclientrust";
const DEFAULT_PORT: u16 = 4222;

pub(crate) fn parse_nats_uri(uri: &str) -> Result<Url> {
    let url = Url::parse(uri)?;
    if url.scheme() != URI_SCHEME {
        Err(err!(UriParseFailure, "Failed to parse NATS URI"))
    } else {
        Ok(url)
    }
}

pub(crate) fn parse_server_uris(uris: &[String]) -> Result<Vec<ServerInfo>> {
    let mut servers = Vec::new();

    for uri in uris {
        let parsed = parse_nats_uri(uri)?;
        let host = parsed
            .host_str()
            .ok_or((
                crate::error::ErrorKind::InvalidClientConfig,
                "Missing host name",
            ))?
            .to_owned();
        let port = parsed.port().unwrap_or(DEFAULT_PORT);
        servers.push(ServerInfo { host, port });
    }

    let mut rng = thread_rng();
    servers.shuffle(&mut rng);

    Ok(servers)
}

pub(crate) fn is_multiline_message(line: &str) -> bool {
    line.starts_with("MSG") || line.starts_with("PUB")
}

pub(crate) fn generate_connect_command(
    info: &ProtocolMessage,
    auth: &AuthenticationStyle,
) -> ProtocolMessage {
    if let AuthenticationStyle::UserCredentials(jwt, seed) = auth {
        if let ProtocolMessage::Info(ref server_info) = info {
            let kp = KeyPair::from_seed(seed.as_ref()).unwrap();
            let nonce = server_info.nonce.clone().unwrap();
            let sigbytes = kp.sign(nonce.as_bytes()).unwrap();
            let sig = Some(data_encoding::BASE64URL_NOPAD.encode(sigbytes.as_slice()));

            let ci = crate::protocol::ConnectionInformation::new(
                false,
                false,
                false,
                None,
                None,
                None,
                "en-us".to_string(),
                "natsclient-rust".to_string(),
                "0.0.1".to_string(),
                Some(1),
                sig,
                Some(jwt.to_string()),
            );
            ProtocolMessage::Connect(ci)
        } else {
            panic!("No server information");
        }
    } else {
        panic!("currently unsupported authentication method");
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProtocolHandler {
    opts: ClientOptions,
    delivery_sender: channel::Sender<DeliveredMessage>,
}

impl ProtocolHandler {
    pub fn new(
        opts: ClientOptions,
        delivery_sender: channel::Sender<DeliveredMessage>,
    ) -> ProtocolHandler {
        ProtocolHandler {
            opts,
            delivery_sender,
        }
    }

    pub fn handle_protocol_message(
        &self,
        pm: &ProtocolMessage,
        sender: &channel::Sender<ProtocolMessage>,
    ) -> Result<()> {
        println!("Received server message: {}", pm);
        match pm {
            ProtocolMessage::Info(server_info) => {
                sender
                    .send(generate_connect_command(pm, &self.opts.authentication))
                    .unwrap();
            }
            ProtocolMessage::Ping => {
                sender.send(ProtocolMessage::Pong).unwrap();
            }
            ProtocolMessage::Message(msg) => {
                self.delivery_sender.send(msg.clone());
            }
            _ => {}
        };
        Ok(())
    }
}
