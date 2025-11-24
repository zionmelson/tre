use std::{io, sync::Arc};

use anyhow::{Context, Result};
use http::Uri;
use native_tls::TlsConnector as NativeTlsConnector;
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

pub async fn connect_tls_endpoint(endpoint: &str) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid endpoint: {endpoint}"))?;

    let tls_builder = NativeTlsConnector::builder();
    let connector = tls_builder
        .build()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    let connector = Arc::new(TlsConnector::from(connector));

    let service_connector = {
        let connector = connector.clone();
        service_fn(move |uri: Uri| {
            let connector = connector.clone();
            async move {
                let host = uri
                    .host()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing host"))?;
                let port = uri.port_u16().unwrap_or(443);
                let addr = format!("{host}:{port}");
                let stream = TcpStream::connect(addr).await?;
                let tls_stream = connector
                    .connect(host, stream)
                    .await
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                Ok::<_, io::Error>(tls_stream)
            }
        })
    };

    endpoint
        .connect_with_connector(service_connector)
        .await
        .map_err(Into::into)
}
