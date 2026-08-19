use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use tracing::warn;

use crate::hub::{ChannelId, Hub, Notif};

const CHANNEL_SIZE: usize = 100;

async fn release<S>(
    stream: &mut S,
    mut cursor: usize,
    chunk_map: &mut HashMap<usize, Arc<[u8]>>,
) -> Result<usize>
where
    S: AsyncWriteExt + std::marker::Unpin,
{
    while let Some(chunk) = chunk_map.remove(&cursor) {
        stream
            .write_all(chunk.as_ref())
            .await
            .context("Failed to write chunk.")?;
        cursor += 1
    }

    Ok(cursor)
}

pub async fn feed<S>(hub: Arc<Mutex<Hub>>, id: String, mut stream: S) -> Result<()>
where
    S: AsyncWriteExt + std::marker::Unpin,
{
    let id = ChannelId::Chunk(id);
    let mut receiver = hub.lock().await.subscribe(&id, CHANNEL_SIZE);
    let mut cursor = None;
    let mut chunk_map = HashMap::new();

    loop {
        match receiver.recv().await {
            Some(Notif::Chunk { chunk_id, chunk }) => {
                let current_cursor = cursor.unwrap_or(chunk_id);
                match current_cursor.cmp(&chunk_id) {
                    Ordering::Equal => {
                        chunk_map.insert(current_cursor, chunk);
                        let new_cursor =
                            release(&mut stream, current_cursor, &mut chunk_map).await?;
                        cursor = Some(new_cursor);
                    }
                    Ordering::Less => {
                        chunk_map.insert(chunk_id, chunk);
                    }
                    Ordering::Greater => {}
                }
            }
            Some(_) => warn!("Received packet on unwanted channel."),
            None => break,
        }
    }
    Ok(())
}
