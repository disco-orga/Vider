use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use rumqttc::{AsyncClient, MqttOptions};
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{debug, error, info, warn};

use crate::{
    configs::{quic_load, tls_load},
    hub::Hub,
    listeners::{quic_listen, tcp_listen, tls_listen},
    lister::{StreamerList, list_loop},
    mqtt::mqtt_listen,
    register::register_to_resolver,
};

mod configs;
mod feeder;
mod hub;
mod ingester;
mod listeners;
mod lister;
mod mqtt;
mod register;
mod topics;

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(long, default_value = "streamhub")]
    server_ip: String,

    #[arg(long, default_value_t = 1666)]
    tcp_port: u16,

    #[arg(long, default_value_t = 1667)]
    tls_port: u16,

    #[arg(long)]
    tls_cert_path: Option<String>,

    #[arg(long)]
    tls_key_path: Option<String>,

    #[arg(long, default_value_t = 1668)]
    quic_port: u16,

    #[arg(long)]
    quic_cert_path: Option<String>,

    #[arg(long)]
    quic_key_path: Option<String>,

    #[arg(long, default_value = "mosquitto")]
    broker_host: String,

    #[arg(long, default_value_t = 1883)]
    broker_port: u16,

    #[arg(long, default_value = "streamhub")]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting StreamHub");

    let args = Args::parse();

    let mqtt_options = MqttOptions::new(args.name.clone(), args.broker_host, args.broker_port);

    let (client, eventloop) = AsyncClient::new(mqtt_options, 5);

    register_to_resolver(
        client.clone(),
        args.name,
        args.server_ip.clone(),
        args.tcp_port,
    )
    .await
    .context("Failed to register StreamHub instance to resolver")?;

    let hub = Arc::new(Mutex::new(Hub::new()));
    let streamer_list = Arc::new(Mutex::new(StreamerList::new()));

    spawn_background_tasks(hub.clone(), streamer_list.clone(), client, eventloop);

    let mut listeners = JoinSet::new();

    listeners.spawn(tcp_listen(
        hub.clone(),
        address(&args.server_ip, args.tcp_port),
    ));

    if let Some((cert, key)) = certificate_paths(args.tls_cert_path, args.tls_key_path, "TLS") {
        let hub = hub.clone();
        let addr = address(&args.server_ip, args.tls_port);

        listeners.spawn(async move {
            let config = tls_load(&cert, &key).context("Failed to load TLS configuration")?;

            tls_listen(hub, addr, config).await
        });
    }

    if let Some((cert, key)) = certificate_paths(args.quic_cert_path, args.quic_key_path, "QUIC") {
        let hub = hub.clone();
        let addr = address(&args.server_ip, args.quic_port);

        listeners.spawn(async move {
            let config = quic_load(&cert, &key).context("Failed to load QUIC configuration")?;

            quic_listen(hub, addr, config).await
        });
    }

    while let Some(result) = listeners.join_next().await {
        result
            .context("Listener task panicked")?
            .context("Listener failed")?;
    }

    Ok(())
}

fn address(host: &str, port: u16) -> String {
    format!("{host}:{port}")
}

fn certificate_paths(
    cert: Option<String>,
    key: Option<String>,
    protocol: &str,
) -> Option<(String, String)> {
    match (cert, key) {
        (Some(cert), Some(key)) => Some((cert, key)),

        (None, None) => {
            debug!("{protocol} configuration not provided; listener disabled");
            None
        }

        _ => {
            error!("{protocol} requires both a certificate path and a private key path");
            None
        }
    }
}

fn spawn_background_tasks(
    hub: Arc<Mutex<Hub>>,
    streamer_list: Arc<Mutex<StreamerList>>,
    client: AsyncClient,
    eventloop: rumqttc::EventLoop,
) {
    tokio::spawn(list_loop(hub, streamer_list.clone()));

    tokio::spawn(async move {
        if let Err(err) = mqtt_listen(client, eventloop, streamer_list).await {
            warn!(%err, "MQTT listener stopped");
        }
    });
}
