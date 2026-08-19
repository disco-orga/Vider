use std::{collections::HashSet, sync::Arc};

use tokio::sync::Mutex;
use tracing::warn;

use crate::hub::{ChannelId, Hub, Notif};

pub type StreamerList = HashSet<String>;

pub async fn list_loop(hub: Arc<Mutex<Hub>>, streamer_list: Arc<Mutex<StreamerList>>) {
    let mut receiver = hub
        .lock()
        .await
        .subscribe_multiple(&vec![ChannelId::StreamOpened, ChannelId::StreamClosed], 100);
    loop {
        match receiver.recv().await {
            Some(Notif::StreamOpened(s)) => {
                streamer_list.lock().await.insert(s);
            }
            Some(Notif::StreamClosed(s)) => {
                streamer_list.lock().await.remove(&s);
            }
            Some(_) => warn!("List loop received notif on an unwanted channel."),
            None => break,
        }
    }
}
