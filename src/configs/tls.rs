use std::{fs::File, io::BufReader, sync::Arc};

use anyhow::{Context, Result};
use tokio_rustls::rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};

pub fn load(cert_path: &str, key_path: &str) -> Result<Arc<ServerConfig>> {
    let cert_file =
        File::open(cert_path).with_context(|| format!("Failed to open certificate {cert_path}"))?;

    let key_file =
        File::open(key_path).with_context(|| format!("Failed to open private key {key_path}"))?;

    let mut cert_reader = BufReader::new(cert_file);
    let mut key_reader = BufReader::new(key_file);

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::io::Result<_>>()
        .context("Failed to parse certificates")?;

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .context("Failed to parse private key")?
        .context("No private key found")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("Certificate/private key mismatch")?;

    Ok(Arc::new(config))
}
