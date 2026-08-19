use std::{fs::File, io::BufReader};

use anyhow::{Context, Result};
use quinn::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub fn load(cert_path: &str, key_path: &str) -> Result<ServerConfig> {
    let cert_file = File::open(cert_path)?;
    let key_file = File::open(key_path)?;

    let mut cert_reader = BufReader::new(cert_file);
    let mut key_reader = BufReader::new(key_file);

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::io::Result<_>>()
        .context("Failed to parse certificate")?;

    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut key_reader)?.context("No private key found")?;

    let config = ServerConfig::with_single_cert(certs, key)
        .context("Failed to create QUIC server config")?;

    Ok(config)
}
