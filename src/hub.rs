use std::sync::Arc;

use notifier_hub::notifier::NotifierHub;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ChannelId {
    Chunk(String),
    StreamOpened,
    StreamClosed,
}

#[derive(Debug, Clone)]
pub enum Notif {
    Chunk { chunk_id: usize, chunk: Arc<[u8]> },
    StreamOpened(String),
    StreamClosed(String),
}

pub type Hub = NotifierHub<Notif, ChannelId>;
