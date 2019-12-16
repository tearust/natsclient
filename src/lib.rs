#![allow(dead_code)]
#[macro_use]
extern crate derive_builder;

#[macro_use]
extern crate log;

use crate::protocol::ServerInfo;
use crate::subs::SubscriptionManager;
use crate::tcp::TcpClient;
use crossbeam_channel::{self as channel, bounded};
use nats_types::{DeliveredMessage, PublishMessage};
use regex::Captures;
use regex::Regex;
use std::{sync::Arc, thread, time::Duration};
pub type Result<T> = std::result::Result<T, crate::error::Error>;

pub use nats_types::DeliveredMessage as Message;

/// Indicates the type of client authentication used by the NATS client
#[derive(Clone, Debug, PartialEq)]
pub enum AuthenticationStyle {
    /// JSON Web Token (JWT)-based authentication using a JWT and a seed (private) key
    UserCredentials(String, String),
    /// Single token based authentication
    Token(String),
    /// Basic authentication with username and password
    Basic { username: String, password: String },
    /// Anonymous (unauthenticated)
    Anonymous,
}

impl AuthenticationStyle {
    pub fn from_credsfile(file: &str) -> Result<AuthenticationStyle> {
        let raw = std::fs::read_to_string(file)?;

        Self::from_credsfile_str(&raw)
    }

    fn from_credsfile_str(raw: &str) -> Result<AuthenticationStyle> {
        let re = Regex::new(CREDSFILE_FORMAT)?;

        let caps: Vec<Captures> = re.captures_iter(&raw).collect();
        let uc =
            AuthenticationStyle::UserCredentials(caps[1][0].to_string(), caps[1][1].to_string());
        Ok(uc)
    }
}

const CREDSFILE_FORMAT: &str =
    r#"\s*(?:(?:[-]{3,}[^\n]*[-]{3,}\n)(.+)(?:\n\s*[-]{3,}[^\n]*[-]{3,}\n))"#;

type MessageHandler = Arc<dyn Fn(&Message) -> Result<()> + Sync + Send>;

/// Options to configure the NATS client. A builder is available so a fluent
/// API can be used to set options
#[derive(Debug, Clone, Builder, PartialEq)]
#[builder(setter(into), default)]
pub struct ClientOptions {
    cluster_uris: Vec<String>,
    authentication: AuthenticationStyle,
    connect_timeout: Duration,
    reconnect_attempts: u8,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            cluster_uris: Vec::new(),
            authentication: AuthenticationStyle::Anonymous,
            connect_timeout: Duration::from_millis(100),
            reconnect_attempts: 3,
        }
    }
}

impl ClientOptions {
    /// Create a new Client Options Builder
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder::default()
    }
}

/// The main entry point for your application to consume NATS services. This client
/// manages connections, connection retries, adjusts to new servers as they enter the
/// cluster, and much more.
#[derive(Clone)]
pub struct Client {
    id: String,
    inbox_wildcard: String,
    opts: ClientOptions,
    servers: Vec<ServerInfo>,
    server_index: usize,
    submgr: SubscriptionManager,
    delivery_sender: channel::Sender<DeliveredMessage>,
    delivery_receiver: channel::Receiver<DeliveredMessage>,
    write_sender: channel::Sender<Vec<u8>>,
    write_receiver: channel::Receiver<Vec<u8>>,
}

impl Client {
    /// Creates a new client from a set of options, which can be created directly
    /// or through a `ClientOptionsBuilder`
    pub fn from_options(opts: ClientOptions) -> Result<Client> {
        let uris = opts.cluster_uris.clone();
        let (ds, dr) = channel::unbounded();
        let (ws, wr) = channel::unbounded();

        let mut nuid = nuid::NUID::new();
        nuid.randomize_prefix();

        let id = nuid.next();

        Ok(Client {
            inbox_wildcard: format!("_INBOX.{}.*", &id),
            id: id.clone(),
            opts,
            servers: protocol::parse_server_uris(&uris)?,
            submgr: SubscriptionManager::new(id, ws.clone()),
            server_index: 0,
            delivery_sender: ds,
            delivery_receiver: dr,
            write_sender: ws,
            write_receiver: wr,
        })
    }

    /// Creates a new client using the default options and the given URL. A client created this way will
    /// attempt to establish an anonymous connection with the given NATS server
    pub fn new(url: &str) -> Result<Client> {
        let opts = ClientOptions::builder()
            .cluster_uris(vec![url.into()])
            .build()
            .unwrap();
        Self::from_options(opts)
    }

    /// Creates a subscription to a new subject. The subject can be a specfic subject
    /// or a wildcard. The handler supplied will be given a reference to delivered messages
    /// as they arrive, and can return a Result to indicate processing failure
    pub fn subscribe<F>(&self, subject: &str, handler: F) -> Result<()>
    where
        F: Fn(&Message) -> Result<()> + Sync + Send,
        F: 'static,
    {
        self.raw_subscribe(subject, None, Arc::new(handler))
    }

    /// Creates a subscription for a queue group, allowing message delivery to be spread
    /// round-robin style across all clients expressing interest in that subject. For more information on how queue groups work,
    /// consult the NATS documentation.
    pub fn queue_subscribe<F>(&self, subject: &str, queue_group: &str, handler: F) -> Result<()>
    where
        F: Fn(&Message) -> Result<()> + Sync + Send,
        F: 'static,
    {
        self.raw_subscribe(subject, Some(queue_group), Arc::new(handler))
    }

    /// Perform a synchronous request by publishing a message on the given subject and waiting
    /// an expiration period indicated by the `timeout` parameter. If the timeout expires before
    /// a reply arrives on the inbox subject, an `Err` result will be returned.
    pub fn request<T>(
        &self,
        subject: T,
        payload: &[u8],
        timeout: std::time::Duration,
    ) -> Result<Message>
    where
        T: AsRef<str>,
    {
        let (sender, receiver) = channel::bounded(1);
        let inbox = self.submgr.add_new_inbox_sub(sender)?;
        self.publish(subject.as_ref(), payload, Some(&inbox))?;
        match receiver.recv_timeout(timeout) {
            Ok(msg) => Ok(msg.clone()),
            Err(e) => Err(err!(Timeout, "Request timeout expired: {}", e)),
        }
    }

    /// Unsubscribe from a subject or wildcard
    pub fn unsubscribe(&self, subject: impl AsRef<str>) -> Result<()> {
        let s = subject.as_ref();
        self.submgr.unsubscribe_by_subject(s)
    }

    /// Asynchronously publish a message. This is a fire-and-forget style message and an `Ok`
    /// result here does not imply that interested parties have received the message, only that
    /// the message was successfully sent to NATS.
    pub fn publish(&self, subject: &str, payload: &[u8], reply_to: Option<&str>) -> Result<()> {
        /*    let pm = ProtocolMessage::Publish(PublishMessage {
            payload: payload.to_vec(),
            payload_size: payload.len(),
            subject: subject.to_string(),
            reply_to,
        }); */
        let vec = PublishMessage::as_vec(subject, reply_to, payload)?;
        match self.write_sender.send(vec) {
            Ok(_) => Ok(()),
            Err(e) => Err(err!(ConcurrencyFailure, "Concurrency failure: {}", e)),
        }
    }

    /// Connect a client to the NATS server(s) indicated by previously supplied configuration
    pub fn connect(&self) -> Result<()> {
        let (s, r) = bounded(1); // Create a thread block until we send the CONNECT preamble

        let tcp_client = TcpClient::new(
            self.servers.clone(),
            self.delivery_sender.clone(),
            self.write_sender.clone(),
            self.write_receiver.clone(),
            self.opts.clone(),
            s,
        );
        tcp_client.connect()?;
        info!("TCP connection established.");

        if r.recv_timeout(self.opts.connect_timeout).is_err() {
            error!("Failed to establish NATS connection within timeout");
            return Err(err!(
                Timeout,
                "Failed to establish connection without timeout"
            ));
        };

        let mgr = self.submgr.clone();
        self.submgr.add_sub(
            &self.inbox_wildcard, // _INBOX.(nuid).*
            None,
            Arc::new(move |msg| {
                if let Some(sender) = mgr.sender_for_inbox(&msg.subject) {
                    sender.send(msg.clone()).unwrap(); // TODO: kill the unwrap
                    mgr.remove_inbox(&msg.subject)
                }
                Ok(())
            }),
        )?;
        self.start_subscription_dispatcher(self.delivery_receiver.clone())
    }

    fn start_subscription_dispatcher(
        &self,
        receiver: channel::Receiver<DeliveredMessage>,
    ) -> Result<()> {
        let c = self.clone();

        thread::spawn(move || {
            loop {
                match receiver.recv() {
                    Ok(msg) => {
                        let handler = c.get_handler(msg.subscription_id as usize);
                        (handler)(&msg).unwrap(); // TODO: handle this properly
                    }
                    Err(e) => {
                        error!("Failed to receive message: {}", e); // TODO: handle this properly
                    }
                }
            }
        });
        Ok(())
    }

    fn get_handler(&self, sid: usize) -> MessageHandler {
        self.submgr.handler_for_sid(sid).unwrap()
    }

    fn raw_subscribe(
        &self,
        subject: &str,
        queue_group: Option<&str>,
        handler: MessageHandler,
    ) -> Result<()> {
        match self.submgr.add_sub(subject, queue_group, handler) {
            Ok(_) => Ok(()),
            Err(e) => Err(err!(SubscriptionFailure, "Subscription failure: {}", e)),
        }
    }
}

impl Default for Client {
    /// Creates a default client, using anonymous authentication and pointing to the localhost NATS server
    fn default() -> Client {
        Client::from_options(ClientOptions::default()).unwrap()
    }
}

#[macro_use]
pub mod error;
mod protocol;
mod subs;
mod tcp;

#[cfg(test)]
mod tests {
    use super::AuthenticationStyle;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn credsfile_parses() {
        let credsfile = r#"-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBNDNRN1NLT0tCT0tYUDc1WVhMWjcyVDZKNDVIVzJKR0ZRWUJFQ1I2VE1FWEZFN1RKSjVBIiwiaWF0IjoxNTU0ODk2OTQ1LCJpc3MiOiJBQU9KV0RRV1pPQkNFTUVWWUQ2VEhPTUVCSExYS0NBMzZGU0dJVUxINFBWRU1ORDVUMjNEUEM0VSIsIm5hbWUiOiJiZW1pc191c2VyIiwic3ViIjoiVUNMRkYzTFBLTTQ3WTZGNkQ3UExMVzU2MzZKU1JDUFhFUUFDTEdVWTZNT01BS1lXMkk2VUFFRUQiLCJ0eXBlIjoidXNlciIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fX19.3aH-hCSTS8z8rg2km7Q_aat5VpwT-t9swSmh3bnVBY_9IV9wE9mjSOUgHE2sq-7pR4HTCpYa0RPrNcgNfaVuBg
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUACGBWJZLVP4CHF7WTY65KT3I4QHAQ5DEZMFAJKTIUIRQPXE6DVMFQEUU
------END USER NKEY SEED------

*************************************************************"#;

        let authstyle = AuthenticationStyle::from_credsfile_str(credsfile);
        assert!(authstyle.is_ok());
        if let AuthenticationStyle::UserCredentials(_jwt, seed) = authstyle.unwrap() {
            assert_eq!(
                "SUACGBWJZLVP4CHF7WTY65KT3I4QHAQ5DEZMFAJKTIUIRQPXE6DVMFQEUU",
                seed
            );
        }
    }
}
