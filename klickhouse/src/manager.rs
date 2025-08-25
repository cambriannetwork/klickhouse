use std::net::SocketAddr;
use tokio::net::ToSocketAddrs;

use crate::{convert::UnitValue, Client, ClientOptions, KlickhouseError};

#[derive(Clone)]
pub struct Manager<T> {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
    prequel: Vec<String>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Manager<T> {
     pub async fn new<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
    ) -> std::io::Result<Self> {
        Ok(Self {
            destination: tokio::net::lookup_host(destination).await?.collect(),
            options,
            prequel: vec![],
            _marker: std::marker::PhantomData,
        })
    }

    pub fn with_prequel(mut self, prequel: impl Into<String>) -> Self {
        self.prequel.push(prequel.into());
        self
    }
}

impl bb8::ManageConnection for Manager<Client> {
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

pub type ClientManager = Manager<Client>;

/// Connection manager for raw TcpConnection pooling with bb8.
/// This provides lower-level connection pooling compared to ConnectionManager.
#[cfg(feature = "connection")]
impl bb8::ManageConnection for Manager<crate::connection::TcpConnection> {
    type Connection = crate::connection::TcpConnection;
    type Error = KlickhouseError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut client = crate::connection::connect(&self.destination[..], self.options.clone()).await?;

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

#[cfg(feature = "connection")]
pub type ConnectionManager = Manager<crate::connection::TcpConnection>;


#[cfg(all(feature = "connection",feature = "tls"))]
#[derive(Clone)]
pub struct TlsConnectionManager {
    destination: Vec<SocketAddr>,
    options: ClientOptions,
    prequel: Vec<String>,
    name: rustls_pki_types::ServerName<'static>,
    connector: tokio_rustls::TlsConnector, 
}

#[cfg(all(feature = "connection",feature = "tls"))]
impl TlsConnectionManager {
     pub async fn new<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        name: rustls_pki_types::ServerName<'static>,
        connector: tokio_rustls::TlsConnector, 
    ) -> std::io::Result<Self> {
        Ok(Self {
            destination: tokio::net::lookup_host(destination).await?.collect(),
            options,
            prequel: vec![],
            name,
            connector,
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
        let mut client = crate::connection::connect_tls(
            &self.destination[..], 
            self.options.clone(),
            self.name.clone(),
            &self.connector,
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