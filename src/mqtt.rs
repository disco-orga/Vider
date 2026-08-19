use std::sync::Arc;

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, Packet, QoS};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::warn;

use crate::{lister::StreamerList, topics};

#[derive(Serialize)]
struct Response {
    streamers: Vec<String>,
}

pub async fn mqtt_listen(
    client: AsyncClient,
    mut eventloop: EventLoop,
    streamer_list: Arc<Mutex<StreamerList>>,
) -> Result<()> {
    client
        .subscribe(topics::LIST_QUERY, QoS::AtLeastOnce)
        .await
        .context("Failed to subscribe to query list")?;

    loop {
        match eventloop.poll().await.context("Failed to poll mqtt")? {
            Event::Incoming(Packet::Publish(p)) => match &p.topic as &str {
                topics::LIST_QUERY => {
                    let streamers = streamer_list
                        .lock()
                        .await
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>();
                    let json = serde_json::to_string(&Response { streamers })
                        .context("Failed to serialize response.")?;
                    client
                        .publish(topics::LIST_RES, QoS::AtLeastOnce, false, json.as_bytes())
                        .await
                        .context("Failed to publish the list over mqtt.")?;
                }
                _ => warn!("Received packed on unwanted mqtt topic."),
            },
            _ => continue,
        }
    }
}
