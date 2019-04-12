#![allow(dead_code)]
#[macro_use]
extern crate derive_builder;
use crate::protocol::{ProtocolMessage, ServerInfo};
use crate::subs::SubscriptionManager;
use crate::tcp::start_comms;
use crossbeam_channel as channel;
use crossbeam_channel::bounded;
use nats_types::DeliveredMessage;
use nats_types::PublishMessage;
use nats_types::SubscribeMessage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

type Result<T> = std::result::Result<T, crate::error::Error>;

pub use nats_types::DeliveredMessage as Message;

#[derive(Clone, Debug, PartialEq)]
pub enum AuthenticationStyle {
    UserCredentials(String, String),
    Token(String),
    Basic { username: String, password: String },
    Anonymous,
}

type MessageHandler = Arc<Fn(&Message) -> Result<()> + Sync + Send>;

#[derive(Debug, Clone, Builder, PartialEq)]
#[builder(setter(into), default)]
pub struct ClientOptions {
    cluster_uris: Vec<String>,
    authentication: AuthenticationStyle,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            cluster_uris: Vec::new(),
            authentication: AuthenticationStyle::Anonymous,
        }
    }
}

impl ClientOptions {
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder::default()
    }
}

#[derive(Clone)]
struct Client {
    opts: ClientOptions,
    //subscriptions: Arc<RwLock<HashMap<usize, MessageHandler>>>,
    servers: Vec<ServerInfo>,
    server_index: usize,
    //current_sid: Arc<AtomicUsize>,
    submgr: SubscriptionManager,
    delivery_sender: channel::Sender<DeliveredMessage>,
    delivery_receiver: channel::Receiver<DeliveredMessage>,
    write_sender: channel::Sender<ProtocolMessage>,
    write_receiver: channel::Receiver<ProtocolMessage>,
}

impl Client {
    pub fn from_options(opts: ClientOptions) -> Result<Client> {
        let uris = opts.cluster_uris.clone();
        let (ds, dr) = channel::unbounded();
        let (ws, wr) = channel::unbounded();

        Ok(Client {
            opts,
            //subscriptions: Arc::new(RwLock::new(HashMap::new())),
            servers: protocol::parse_server_uris(uris.as_slice())?,
            submgr: SubscriptionManager::new(ws.clone()),
            server_index: 0,
            delivery_sender: ds,
            delivery_receiver: dr,
            write_sender: ws,
            write_receiver: wr,
            //current_sid: Arc::new(AtomicUsize::new(1)),
        })
    }

    pub fn new() -> Result<Client> {
        let opts = ClientOptions::builder()
            .cluster_uris(vec!["nats://0.0.0.0:4222".into()])
            .build()
            .unwrap();
        Self::from_options(opts)
    }

    pub fn subscribe<T, F>(&self, subject: T, handler: F) -> Result<()>
    where
        F: Fn(&Message) -> Result<()> + Sync + Send,
        F: 'static,
        T: Into<String> + Clone,
    {
        self.raw_subscribe(subject, None, Arc::new(handler))
    }

    pub fn queue_subscribe<T, F>(
        &self,
        subject: T,
        queue_group: T,
        handler: MessageHandler,
    ) -> Result<()>
    where
        T: Into<String> + Clone,
    {
        self.raw_subscribe(subject, Some(queue_group.into()), handler)
    }

    pub fn request<T>(
        &self,
        subject: T,
        payload: &[u8],
        timeout: std::time::Duration,
    ) -> Result<Message>
    where
        T: AsRef<str>,
    {
        let (send, recv) = channel::bounded(1);
        let (inbox, sid) = self.submgr.add_new_inbox_sub(Arc::new(move |msg| {
            println!("Received reply on inbox: {}", msg);
            send.send(msg.clone()).unwrap();
            Ok(())
        }))?;
        self.publish(subject.as_ref(), payload, Some(inbox.clone()))?;
        let res = match recv.recv_timeout(timeout) {
            Ok(msg) => Ok(msg.clone()),
            Err(e) => Err(err!(Timeout, "Request timeout expired: {}", e)),
        };
        self.submgr.unsubscribe(sid, None)?;
        res
    }

    pub fn unsubscribe(&self, subject: impl AsRef<str>) -> Result<()> {
        let s = subject.as_ref();
        self.submgr.unsubscribe_by_subject(s)
    }

    pub fn publish(&self, subject: &str, payload: &[u8], reply_to: Option<String>) -> Result<()> {
        let pm = ProtocolMessage::Publish(PublishMessage {
            payload: payload.to_vec(),
            payload_size: payload.len(),
            subject: subject.to_string(),
            reply_to,
        });
        match self.write_sender.send(pm) {
            Ok(_) => Ok(()),
            Err(e) => Err(err!(ConcurrencyFailure, "Concurrency failure: {}", e)),
        }
    }

    pub fn connect(&self) -> Result<()> {
        let (host, port) = {
            let server_info = &self.servers[self.server_index];
            (server_info.host.to_string(), server_info.port)
        };

        let (s, r) = bounded(1); // Create a thread block until we send the CONNECT preamble

        start_comms(
            &host,
            port,
            self.delivery_sender.clone(),
            self.write_sender.clone(),
            self.write_receiver.clone(),
            self.opts.clone(),
            s,
        )?;
        r.recv_timeout(std::time::Duration::from_millis(30))
            .unwrap(); // TODO: handle "no connection sent within 30ms"
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
                        println!("Failed to receive message: {}", e); // TODO: handle this properly
                    }
                }
            }
        });
        Ok(())
    }

    fn get_handler(&self, sid: usize) -> MessageHandler {
        self.submgr.handler_for_sid(sid).unwrap()
    }

    fn raw_subscribe<T: Into<String> + Clone>(
        &self,
        subject: T,
        queue_group: Option<String>,
        handler: MessageHandler,
    ) -> Result<()> {
        match self.submgr.add_sub(subject, queue_group, handler) {
            Ok(_) => Ok(()),
            Err(e) => Err(err!(SubscriptionFailure, "Subscription failure: {}", e)),
        }
    }
}

#[macro_use]
pub mod error;
mod protocol;
mod subs;
mod tcp;

#[cfg(test)]
mod tests {
    use super::{AuthenticationStyle, Client, ClientOptions};
    use std::time::Duration;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn ergonomics_1() {
        match ergs1() {
            Ok(_) => {}
            Err(e) => {
                println!("ERROR: {}", e);
                assert!(false);
            }
        }
    }

    fn ergs1() -> Result<(), Box<dyn std::error::Error>> {
        let jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBNDNRN1NLT0tCT0tYUDc1WVhMWjcyVDZKNDVIVzJKR0ZRWUJFQ1I2VE1FWEZFN1RKSjVBIiwiaWF0IjoxNTU0ODk2OTQ1LCJpc3MiOiJBQU9KV0RRV1pPQkNFTUVWWUQ2VEhPTUVCSExYS0NBMzZGU0dJVUxINFBWRU1ORDVUMjNEUEM0VSIsIm5hbWUiOiJiZW1pc191c2VyIiwic3ViIjoiVUNMRkYzTFBLTTQ3WTZGNkQ3UExMVzU2MzZKU1JDUFhFUUFDTEdVWTZNT01BS1lXMkk2VUFFRUQiLCJ0eXBlIjoidXNlciIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fX19.3aH-hCSTS8z8rg2km7Q_aat5VpwT-t9swSmh3bnVBY_9IV9wE9mjSOUgHE2sq-7pR4HTCpYa0RPrNcgNfaVuBg";
        let seed = "SUACGBWJZLVP4CHF7WTY65KT3I4QHAQ5DEZMFAJKTIUIRQPXE6DVMFQEUU";
        let opts = ClientOptions::builder()
            .cluster_uris(vec!["nats://localhost:4222".into()])
            .authentication(AuthenticationStyle::UserCredentials(
                jwt.to_string(),
                seed.to_string(),
            ))
            .build()?;

        let client = Client::from_options(opts)?;
        client.connect()?;

        let c = client.clone();
        client.subscribe("usage", move |msg| {
            let reply_to = msg
                .reply_to
                .as_ref()
                .map_or("random".to_string(), |r| r.to_string());
            println!("Received a usage message: {}", msg);
            c.publish(
                &reply_to,
                r#"{"usage": "$4324823903 USD"}"#.as_bytes(),
                None,
            )?;
            Ok(())
        })?;

        let msg = client.request(
            "usage",
            r#"{"customer_id": 34}"#.as_bytes(),
            Duration::from_millis(300),
        )?;
        println!("Response from request: {}", msg);

        //let res = client.publish("usage", r#"{"customer_id": "12"}"#.as_bytes(), None);
        //assert!(res.is_ok());

        std::thread::sleep(Duration::from_secs(200));

        Ok(())
    }
}
