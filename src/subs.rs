use crate::{Message, Result};
use crossbeam_channel as channel;
use crossbeam_channel::Sender;
use nats_types::{SubscribeMessage, UnsubscribeMessage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

type MessageHandler = Arc<dyn Fn(&Message) -> Result<()> + Sync + Send>;

const NUID_LENGTH: usize = 22;
const INBOX_PREFIX: &'static str = "_INBOX.";

pub(crate) struct Subscription {
    id: usize,
    subject: String,
    handler: MessageHandler,
}

#[derive(Clone)]
pub(crate) struct SubscriptionManager {
    client_id: String,
    subs: Arc<RwLock<HashMap<usize, Subscription>>>,
    inboxes: Arc<RwLock<HashMap<String, Sender<Message>>>>,
    sender: channel::Sender<Vec<u8>>,
    current_sid: Arc<AtomicUsize>,
}

impl SubscriptionManager {
    pub fn new(client_id: String, sender: channel::Sender<Vec<u8>>) -> SubscriptionManager {
        SubscriptionManager {
            client_id,
            subs: Arc::new(RwLock::new(HashMap::new())),
            sender,
            current_sid: Arc::new(AtomicUsize::new(1)),
            inboxes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_new_inbox_sub(&self, sender: Sender<Message>) -> Result<String> {
        let subject = new_inbox(&self.client_id);
        let mut inboxes = self.inboxes.write().unwrap();
        inboxes.insert(subject.clone(), sender);

        Ok(subject)
    }

    pub fn sender_for_inbox(&self, inbox: &str) -> Option<Sender<Message>> {
        let inboxes = self.inboxes.read().unwrap();
        if inboxes.contains_key(inbox) {
            let sender = &inboxes[inbox];
            Some(sender.clone())
        } else {
            None
        }
    }

    pub fn remove_inbox(&self, inbox: &str) {
        let mut inboxes = self.inboxes.write().unwrap();
        inboxes.remove(inbox);
    }

    pub fn add_sub(
        &self,
        subject: &str,
        queue_group: Option<&str>,
        handler: MessageHandler,
    ) -> Result<usize> {
        let mut subs = self.subs.write().unwrap();
        let sid = self.next_sid();
        let vec = SubscribeMessage::as_vec(subject, queue_group, sid)?;
        self.sender.send(vec)?;
        subs.insert(
            sid,
            Subscription {
                id: sid,
                subject: subject.to_string(),
                handler: handler,
            },
        );
        Ok(sid)
    }

    pub fn unsubscribe(&self, sid: usize, max_msgs: Option<usize>) -> Result<()> {
        let mut subs = self.subs.write().unwrap();
        let vec = UnsubscribeMessage::as_vec(sid, max_msgs)?;
        self.sender.send(vec)?;
        subs.remove(&sid);
        Ok(())
    }

    pub fn unsubscribe_by_subject(&self, subject: &str) -> Result<()> {
        let sid = self.sid_for_subject(subject)?;
        self.unsubscribe(sid, None)
    }

    pub fn handler_for_sid(&self, sid: usize) -> Result<MessageHandler> {
        let subs = self.subs.read().unwrap();
        Ok(subs[&sid].handler.clone())
    }

    fn next_sid(&self) -> usize {
        self.current_sid.fetch_add(1, Ordering::Relaxed)
    }

    fn sid_for_subject(&self, subject: &str) -> Result<usize> {
        let subs = self.subs.read().unwrap();
        for (k, v) in subs.iter() {
            if v.subject == *subject {
                return Ok(*k);
            }
        }
        Err(err!(SubscriptionFailure, "No such subject: {}", subject))
    }
}

fn new_inbox(client_id: &str) -> String {
    format!("{}{}.{}", INBOX_PREFIX, client_id, nuid::next()) // _INBOX.(nuid).(nuid)
}

#[cfg(test)]
mod tests {
    use super::SubscriptionManager;
    use crate::{protocol::ProtocolMessage, Message};
    use crossbeam_channel as channel;
    use nats_types::{SubscribeMessage, UnsubscribeMessage};
    use std::sync::Arc;

    #[test]
    fn add_subscription_sends_sub_message() {
        let (sender, r) = channel::unbounded();

        let sm = SubscriptionManager::new("test".to_string(), sender);

        sm.add_sub("test", None, Arc::new(msg_handler)).unwrap();
        let sub_message = r.recv().unwrap();

        assert_eq!(
            String::from_utf8(sub_message).unwrap(),
            ProtocolMessage::Subscribe(SubscribeMessage {
                queue_group: None,
                subject: "test".to_string(),
                subscription_id: 1,
            })
            .to_string()
        );
    }

    #[test]
    fn remove_subscription_sends_unsub_message() {
        let (sender, r) = channel::unbounded();

        let sm = SubscriptionManager::new("test".to_string(), sender);

        let sid = sm.add_sub("test", None, Arc::new(msg_handler)).unwrap();
        let _ = r.recv().unwrap();
        sm.unsubscribe(sid, None).unwrap();
        let unsub_message = r.recv().unwrap();
        assert_eq!(
            String::from_utf8(unsub_message).unwrap(),
            ProtocolMessage::Unsubscribe(UnsubscribeMessage {
                max_messages: None,
                subscription_id: sid,
            })
            .to_string()
        );
    }

    fn msg_handler(_msg: &Message) -> super::Result<()> {
        Ok(())
    }
}
