use std::net::SocketAddr;
use tokio::net::ToSocketAddrs;

use crate::{convert::UnitValue, Client, ClientOptions, KlickhouseError};

#[derive(Clone)]
pub struct ClientManager {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
    prequel: Vec<String>,
}

impl ClientManager {
    pub async fn new<A: ToSocketAddrs>(destination: A, options: ClientOptions) -> std::io::Result<Self> {
        Ok(Self {
            destination: tokio::net::lookup_host(destination).await?.collect(),
            options,
            prequel: vec![],
        })
    }

    pub fn with_prequel(mut self, prequel: impl Into<String>) -> Self {
        self.prequel.push(prequel.into());
        self
    }
}

impl bb8::ManageConnection for ClientManager {
    type Connection = Client;
    type Error = KlickhouseError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let client = Client::connect(&self.destination[..], self.options.clone()).await?;

        // Run prequel queries if specified
        for prequel in &self.prequel {
            client.execute(prequel).await?;
        }

        Ok(client)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let _ = conn.query_one::<UnitValue<String>>("select '';").await?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }
}

#[cfg(feature = "connection")]
pub struct TcpConnectionManager {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
    prequel: Vec<String>,
    info: crate::connection::ClientInfo<'static>,
}

#[cfg(feature = "connection")]
impl TcpConnectionManager {
    pub async fn new<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        info: Option<crate::connection::ClientInfo<'static>>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            destination: tokio::net::lookup_host(destination).await?.collect(),
            options,
            prequel: vec![],
            info: info.unwrap_or(crate::connection::DEFAULT_CLIENT_INFO),
        })
    }

    pub fn with_prequel(mut self, prequel: impl Into<String>) -> Self {
        self.prequel.push(prequel.into());
        self
    }
}

/// Connection manager for raw TcpConnection pooling with bb8.
/// This provides lower-level connection pooling compared to ConnectionManager.
#[cfg(feature = "connection")]
impl bb8::ManageConnection for TcpConnectionManager {
    type Connection = crate::connection::TcpConnection;
    type Error = KlickhouseError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        use crate::TcpConnection;

        let mut client = TcpConnection::connect_with_info(&self.destination[..], self.options.clone(),self.info.clone()).await?;

        // Run prequel queries if specified
        for prequel in &self.prequel {
            client.execute(prequel).await?;
        }

        Ok(client)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Use ping_pong for health check as it's more efficient than a query
        conn.ping_pong().await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        // TcpConnection doesn't have an is_closed method like Client,
        // so we rely on is_valid for connection health checking
        false
    }
}


#[cfg(all(feature = "connection",feature = "tls"))]
#[derive(Clone)]
pub struct TlsConnectionManager {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
    prequel: Vec<String>,
    name: rustls_pki_types::ServerName<'static>,
    connector: tokio_rustls::TlsConnector, 
    info: crate::connection::ClientInfo<'static>,
}

#[cfg(all(feature = "connection",feature = "tls"))]
impl TlsConnectionManager {
     pub async fn new<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        name: rustls_pki_types::ServerName<'static>,
        connector: tokio_rustls::TlsConnector, 
        info: Option<crate::connection::ClientInfo<'static>>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            destination: tokio::net::lookup_host(destination).await?.collect(),
            options,
            prequel: vec![],
            name,
            connector,
            info: info.unwrap_or(crate::connection::DEFAULT_CLIENT_INFO),
        })
    }

    pub fn with_prequel(mut self, prequel: impl Into<String>) -> Self {
        self.prequel.push(prequel.into());
        self
    }
}

/// Connection manager for raw TcpConnection pooling with bb8.
/// This provides lower-level connection pooling compared to ConnectionManager.
#[cfg(all(feature = "connection",feature = "tls"))]
impl bb8::ManageConnection for TlsConnectionManager {
    type Connection = crate::connection::TlsConnection;
    type Error = KlickhouseError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut client = crate::connection::TlsConnection::connect_with_info(
            &self.destination[..], 
            self.options.clone(),
            self.name.clone(),
            &self.connector,
            self.info.clone()
        ).await?;

        // Run prequel queries if specified
        for prequel in &self.prequel {
            client.execute(prequel).await?;
        }

        Ok(client)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Use ping_pong for health check as it's more efficient than a query
        conn.ping_pong().await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        // TcpConnection doesn't have an is_closed method like Client,
        // so we rely on is_valid for connection health checking
        false
    }
}