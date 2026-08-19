use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::{io::AsyncReadExt, sync::Mutex};
use tracing::warn;

use crate::hub::{ChannelId, Hub, Notif};

const CHUNK_SIZE: usize = 1024;

pub async fn send_on_hub(hub: &Arc<Mutex<Hub>>, msg: Notif, id: &ChannelId) {
    if let Err(e) = hub.lock().await.clone_send(msg, id) {
        warn!("Failed to publish: {e}");
    }
}

pub async fn ingest<S>(hub: Arc<Mutex<Hub>>, id: String, mut stream: S) -> Result<()>
where
    S: AsyncReadExt + std::marker::Unpin,
{
    send_on_hub(
        &hub,
        Notif::StreamOpened(id.clone()),
        &ChannelId::StreamOpened,
    )
    .await;

    let channel_id = ChannelId::Chunk(id.clone());

    let result: Result<()> = async {
        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut chunk_id = 1;

        loop {
            let n = stream.read(&mut buf).await?;

            if n == 0 {
                break;
            }

            let chunk: Arc<[u8]> = Arc::from(&buf[..n]);
            let notif = Notif::Chunk { chunk_id, chunk };

            hub.lock()
                .await
                .clone_send(notif, &channel_id)
                .context("Failed to broadcast chunk.")?;

            chunk_id += 1;
        }

        Ok(())
    }
    .await;

    send_on_hub(&hub, Notif::StreamClosed(id), &ChannelId::StreamClosed).await;

    result
}
