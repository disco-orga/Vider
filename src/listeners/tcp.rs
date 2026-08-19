use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::{net::TcpListener, sync::Mutex};

use crate::{hub::Hub, listeners::start_job};

pub async fn listen(hub: Arc<Mutex<Hub>>, addr: String) -> Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .context(format!("Failed to connect to {addr}"))?;

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("Failed to accept connection.")?;

        start_job(hub.clone(), stream);
    }
}
