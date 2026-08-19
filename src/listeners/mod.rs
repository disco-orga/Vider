mod quic;
mod tcp;
mod tls;

use crate::{feeder::feed, ingester::ingest};
use std::sync::Arc;

use anyhow::{Context, Result};
pub use quic::listen as quic_listen;
use serde::Deserialize;
pub use tcp::listen as tcp_listen;
pub use tls::listen as tls_listen;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
    task::JoinHandle,
};
use tracing::{error, warn};

use crate::hub::Hub;

const HEADER_LENGTH: usize = std::mem::size_of::<u32>();
const MAX_HEADER_SIZE: usize = 16 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum StreamKind {
    Subscribe,
    Publish,
}

#[derive(Deserialize)]
struct Header {
    kind: StreamKind,
    stream_id: String,
}

async fn read_header<S>(stream: &mut S) -> Result<Header>
where
    S: AsyncReadExt + std::marker::Unpin,
{
    let mut header_length_bytes = [0; HEADER_LENGTH];

    stream
        .read_exact(&mut header_length_bytes)
        .await
        .context(format!("Streamer connection closed"))?;

    let header_length = u32::from_be_bytes(header_length_bytes) as usize;

    if header_length > MAX_HEADER_SIZE {
        anyhow::bail!("Header too large: {header_length}");
    }

    let mut header_bytes = vec![0; header_length];

    stream
        .read_exact(&mut header_bytes)
        .await
        .context(format!("Failed to read complete header."))?;

    let header_string =
        String::from_utf8(header_bytes).context("Failed to read the header as a string")?;

    let header =
        serde_json::from_str::<Header>(&header_string).context("Failed to deserialize header.")?;

    Ok(header)
}

fn start_job<S>(hub: Arc<Mutex<Hub>>, mut stream: S) -> JoinHandle<()>
where
    S: AsyncReadExt + AsyncWriteExt + std::marker::Unpin + std::marker::Send + 'static,
{
    tokio::spawn(async move {
        let header = match read_header(&mut stream).await {
            Ok(header) => header,
            Err(e) => {
                error!("{e}");
                return;
            }
        };

        if let Err(e) = match header.kind {
            StreamKind::Subscribe => feed(hub, header.stream_id, stream).await,
            StreamKind::Publish => ingest(hub, header.stream_id, stream).await,
        } {
            warn!("{e}")
        };
    })
}
