use tokio::time::{Duration, sleep};

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, QoS};
use serde::Serialize;
use serde_json::{Value, json};
use tracing::{debug, error, info};

use crate::topics;

#[derive(Serialize)]
pub struct Informations {
    pub service: String,
    pub host: String,
    pub port: u16,
    pub ttl_ms: Option<u64>,
    pub metadatas: Value,
}

#[derive(Serialize)]
pub struct Heartbeat {
    pub service: String,
}

const TTL_MS: u64 = 3000;
const HEARTBEAT_REFRESH: u64 = (TTL_MS as f32 * 0.7) as u64;

async fn heartbeat_loop(name: String, client: AsyncClient, heartbeat_refresh: u64) -> Result<()> {
    let heartbeat = Heartbeat { service: name };
    let payload = serde_json::to_string(&heartbeat)
        .context("Failed to serialize heartbeat payload to JSON")?;

    loop {
        sleep(Duration::from_millis(heartbeat_refresh)).await;

        debug!("Sending heartbeat to resolver");

        client
            .publish(
                topics::HEARTBEAT,
                QoS::AtLeastOnce,
                false,
                payload.as_bytes(),
            )
            .await
            .context("Failed to publish heartbeat message to MQTT broker")?;
    }
}

pub async fn register_to_resolver(
    client: AsyncClient,
    name: String,
    host: String,
    port: u16,
) -> Result<()> {
    let topic = topics::REGISTER;

    info!("Registering service to resolver on topic '{topic}'");

    let infos = Informations {
        service: name.clone(),
        host,
        port,
        metadatas: json!(""),
        ttl_ms: Some(TTL_MS),
    };

    let payload = serde_json::to_string(&infos)
        .context("Failed to serialize service registration payload to JSON.")?;

    client
        .publish(topic, QoS::AtLeastOnce, false, payload.as_bytes())
        .await
        .context("Failed to publish service registration to resolver")?;

    tokio::spawn(async move {
        if let Err(error) = heartbeat_loop(name, client, HEARTBEAT_REFRESH).await {
            error!("Heartbeat loop terminated with an error: {error}");
        }
    });

    info!("Service successfully registered to resolver");

    Ok(())
}
