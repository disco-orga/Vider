use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::{net::TcpListener, sync::Mutex};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

use crate::{hub::Hub, listeners::start_job};

pub async fn listen(
    hub: Arc<Mutex<Hub>>,
    addr: String,
    tls_config: Arc<ServerConfig>,
) -> Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {addr}"))?;

    let acceptor = TlsAcceptor::from(tls_config);

    loop {
        let (stream, peer_addr) = listener
            .accept()
            .await
            .context("Failed to accept TCP connection")?;

        let acceptor = acceptor.clone();
        let hub = hub.clone();

        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    start_job(hub, stream);
                }

                Err(err) => {
                    tracing::warn!(
                        %peer_addr,
                        %err,
                        "TLS handshake failed"
                    );
                }
            }
        });
    }
}
