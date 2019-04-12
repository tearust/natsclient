use crate::{Message, ProtocolMessage, Result};
use crossbeam_channel as channel;
use nats_types::{SubscribeMessage, UnsubscribeMessage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

type MessageHandler = Arc<Fn(&Message) -> Result<()> + Sync + Send>;

const NUID_LENGTH: usize = 22;
const INBOX_PREFIX: &'static str = "_INBOX.";

pub(crate) struct Subscription {
    id: usize,
    subject: String,
    handler: MessageHandler,
}

#[derive(Clone)]
pub(crate) struct SubscriptionManager {
    subs: Arc<RwLock<HashMap<usize, Subscription>>>,
    sender: channel::Sender<ProtocolMessage>,
    current_sid: Arc<AtomicUsize>,
}

impl SubscriptionManager {
    pub fn new(sender: channel::Sender<ProtocolMessage>) -> SubscriptionManager {
        SubscriptionManager {
            subs: Arc::new(RwLock::new(HashMap::new())),
            sender,
            current_sid: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn add_new_inbox_sub(&self, handler: MessageHandler) -> Result<(String, usize)> {
        let subject = new_inbox();
        match self.add_sub(subject.clone(), None, handler) {
            Ok(sid) => Ok((subject, sid)),
            Err(e) => Err(err!(SubscriptionFailure, "Subscription failure: {}", e)),
        }
    }

    pub fn add_sub(
        &self,
        subject: impl Into<String>,
        queue_group: Option<String>,
        handler: MessageHandler,
    ) -> Result<usize> {
        let mut subs = self.subs.write().unwrap();
        let subject: String = subject.into();
        let sid = self.next_sid();
        self.sender
            .send(ProtocolMessage::Subscribe(SubscribeMessage {
                queue_group,
                subject: subject.clone(),
                subscription_id: sid,
            }))?;
        subs.insert(
            sid,
            Subscription {
                id: sid,
                subject: subject,
                handler: handler,
            },
        );
        Ok(sid)
    }

    pub fn unsubscribe(&self, sid: usize, max_msgs: Option<usize>) -> Result<()> {
        let mut subs = self.subs.write().unwrap();
        self.sender
            .send(ProtocolMessage::Unsubscribe(UnsubscribeMessage {
                subscription_id: sid,
                max_messages: max_msgs,
            }))?;
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

fn new_inbox() -> String {
    format!("{}{}", INBOX_PREFIX, nuid::next())
}

#[cfg(test)]
mod tests {
    use super::SubscriptionManager;
    use crate::{Message, ProtocolMessage};
    use crossbeam_channel as channel;
    use nats_types::{SubscribeMessage, UnsubscribeMessage};
    use std::sync::Arc;

    #[test]
    fn add_subscription_sends_sub_message() {
        let (sender, r) = channel::unbounded();

        let sm = SubscriptionManager::new(sender);

        sm.add_sub("test", None, Arc::new(msg_handler)).unwrap();
        let sub_message = r.recv().unwrap();
        assert_eq!(
            sub_message,
            ProtocolMessage::Subscribe(SubscribeMessage {
                queue_group: None,
                subject: "test".to_string(),
                subscription_id: 1,
            })
        );
    }

    #[test]
    fn remove_subscription_sends_unsub_message() {
        let (sender, r) = channel::unbounded();

        let sm = SubscriptionManager::new(sender);

        let sid = sm.add_sub("test", None, Arc::new(msg_handler)).unwrap();
        let _ = r.recv().unwrap();
        sm.unsubscribe(sid, None).unwrap();
        let unsub_message = r.recv().unwrap();
        assert_eq!(
            unsub_message,
            ProtocolMessage::Unsubscribe(UnsubscribeMessage {
                max_messages: None,
                subscription_id: sid,
            })
        );
    }

    #[test]
    fn remove_subscription_for_inbox() {
        // Mimic request behavior by subscribing to an inbox and unsubscribing
        // from the inbox name instead of the sid.
        let (sender, r) = channel::unbounded();

        let sm = SubscriptionManager::new(sender);

        let (inbox, sid) = sm.add_new_inbox_sub(Arc::new(msg_handler)).unwrap();

        let submsg = r.recv().unwrap(); // the outbound SUB message
        assert_eq!(
            submsg,
            ProtocolMessage::Subscribe(SubscribeMessage {
                queue_group: None,
                subject: inbox.clone(),
                subscription_id: sid,
            })
        );
        let unsub = sm.unsubscribe_by_subject(&inbox);
        let unsubmsg = r.recv().unwrap(); // outbound UNSUB message
        assert_eq!(
            unsubmsg,
            ProtocolMessage::Unsubscribe(UnsubscribeMessage {
                max_messages: None,
                subscription_id: sid,
            })
        );
        assert!(unsub.is_ok());
    }

    fn msg_handler(msg: &Message) -> super::Result<()> {
        Ok(())
    }
}
