use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context as _, Result};
use quinn::{Endpoint, ServerConfig};
use tokio::{net::lookup_host, sync::Mutex};

use crate::{hub::Hub, listeners::start_job};

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use quinn::{RecvStream, SendStream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct QuicStream {
    send: SendStream,
    recv: RecvStream,
}

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.send).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => Poll::Ready(Ok(n)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(std::io::Error::other(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.send.finish() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(err) => Poll::Ready(Err(std::io::Error::other(err))),
        }
    }
}

async fn resolve_addr(addr: &str) -> Result<SocketAddr> {
    if let Ok(addr) = addr.parse() {
        return Ok(addr);
    }

    lookup_host(addr)
        .await
        .with_context(|| format!("Failed to resolve {addr}"))?
        .next()
        .with_context(|| format!("No address found for {addr}"))
}

pub async fn listen(hub: Arc<Mutex<Hub>>, addr: String, server_config: ServerConfig) -> Result<()> {
    let addr = resolve_addr(&addr).await?;

    let endpoint =
        Endpoint::server(server_config, addr).context("Failed to create QUIC endpoint")?;

    while let Some(incoming) = endpoint.accept().await {
        let hub = hub.clone();

        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(connection) => connection,
                Err(err) => {
                    tracing::warn!(%err, "QUIC handshake failed");
                    return;
                }
            };

            loop {
                let stream = connection.accept_bi().await;

                let (send, recv) = match stream {
                    Ok(stream) => stream,
                    Err(err) => {
                        tracing::debug!(%err, "QUIC connection closed");
                        return;
                    }
                };

                start_job(hub.clone(), QuicStream { send, recv });
            }
        });
    }

    Ok(())
}
