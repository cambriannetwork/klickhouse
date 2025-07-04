use futures_util::{pin_mut, stream, Stream, StreamExt};

use indexmap::IndexMap;
use log::error;

use tokio::{
    io::{BufReader, BufWriter}, net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream, ToSocketAddrs
    }, sync::broadcast
};


use uuid::Uuid;

use crate::{
    block::{Block, BlockInfo}, internal_client_in::InternalClientIn, internal_client_out::{
        ClientHello, 
        ClientInfo, 
        InternalClientOut, 
        Query, QueryKind, 
        QueryProcessingStage
    }, io::{ClickhouseRead, ClickhouseWrite}, protocol::{self, CompressionMethod, ServerPacket}, ClientOptions, KlickhouseError, ParsedQuery, Progress, Result, Row, Type, Value
};

// Maximum number of progress statuses to keep in memory. New statuses evict old ones.
const PROGRESS_CAPACITY: usize = 100;

// Default client info used for connections that do not specify their own.
pub const DEFAULT_CLIENT_INFO: ClientInfo<'static> = ClientInfo {
                    kind: QueryKind::InitialQuery,
                    initial_user: "",
                    initial_query_id: "",
                    initial_address: "0.0.0.0:0",
                    os_user: "",
                    client_hostname: "localhost",
                    client_name: "ClickHouseclient",
                    client_version_major: crate::VERSION_MAJOR,
                    client_version_minor: crate::VERSION_MINOR,
                    client_tcp_protocol_version: protocol::DBMS_TCP_PROTOCOL_VERSION,
                    quota_key: "",
                    distributed_depth: 1,
                    client_version_patch: 1,
                    open_telemetry: None,
                };


pub struct Connection<R: ClickhouseRead, W: ClickhouseWrite> {
    input: InternalClientIn<R>,
    output: InternalClientOut<W>,
    progress: broadcast::Sender<Progress>,
    info: ClientInfo<'static>,
}



/// This is lightweight connection to Clickhouse.
/// It is not thread safe and should not be shared between threads. 
/// It is used to send queries and receive responses.
impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> Connection<R, W> {

    async fn new(reader: R, writer: W, options: ClientOptions, info:ClientInfo<'static>) -> Result<Self> {
        let mut input = InternalClientIn::new(reader);
        let mut output = InternalClientOut::new(writer);
        
        output
        .send_hello(ClientHello {
            default_database: &options.default_database,
            username: &options.username,
            password: &options.password,
        })
        .await?;
        let hello_response = input.receive_hello().await?;
        input.server_hello = hello_response.clone();
        output.server_hello = hello_response.clone();
        Ok(Self {
            input,
            output,
            progress: broadcast::channel(PROGRESS_CAPACITY).0,
            info,
        })
    }

    async fn send_empty_block(&mut self) -> Result<()> {
        self.output
            .send_data(
                Block {
                    info: BlockInfo::default(),
                    rows: 0,
                    column_types: IndexMap::new(),
                    column_data: IndexMap::new(),
                },
                CompressionMethod::default(),
                "",
                false,
            )
            .await
    }

    async fn send_data(&mut self, block: Block) -> Result<()> {
        self.output
            .send_data(block, CompressionMethod::default(), "", false)
            .await
    }
    
    async fn send_query(&mut self, query:impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Result<()> {
        let parsed_query:ParsedQuery = query.try_into()?;
        let query = parsed_query.query.trim().to_string();
       

        let mut query_id = &Uuid::new_v4().to_string();
        if let Some(id) = parsed_query.id() {
            query_id = id;
        } 

        self.output
            .send_query(Query {
                id:query_id,
                info:self.info.clone(),
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::default(),
                query: &query,
            })
            .await.map_err(|e| {
                KlickhouseError::ProtocolError(format!("failed to send query: {e}"))
            })?;
        self.send_empty_block().await
    }

    async fn receive_block(&mut self) -> Option<Result<Block>> {
        loop {
            match self.input.receive_packet().await {
                Ok(ServerPacket::Data(block)) => return Some(Ok(block.block)),
                Ok(ServerPacket::EndOfStream) => return None,
                Ok(ServerPacket::Exception(e)) => return Some(Err(e.emit())),
                Ok(ServerPacket::Progress(p)) => {
                    let _ = self.progress.send(p);
                    continue;
                }
                Ok(_) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn discard_blocks(&mut self) -> Result<()> {
        loop {
            match self.input.receive_packet().await {
                Ok(ServerPacket::EndOfStream) => return Ok(()),
                Ok(ServerPacket::Exception(e)) => return Err(e.emit()),
                Ok(ServerPacket::Progress(p)) => {
                    let _ = self.progress.send(p);
                    continue;
                }
                Ok(_) => continue,
                Err(e) => return Err(e),
            }
        }
    }


    fn receive_blocks<'a>(&'a mut self) -> Result<impl Stream<Item = Result<Block>> + Send +  'a> {
        let stream = futures_util::stream::unfold(self, |this| async {
            match this.receive_block().await {
                Some(Ok(block)) => Some((Ok(block), this)),
                Some(Err(e)) => Some((Err(e), this)),
                None => None,
            }
        });
        Ok(stream)
    }

    /// Sends a ping to the server and waits for a pong to check if the connection is alive.
    pub async fn ping_pong(&mut self) -> Result<()> {
        self.output.send_ping().await?;
        match self.input.receive_packet().await {
            Ok(ServerPacket::Pong) => Ok(()),
            Ok(_) => Err(KlickhouseError::ProtocolError("unexpected packet from server".to_string())),
            Err(e) => Err(e),
        }
    }


    /// Sends a query string over native protocol and returns a stream of blocks.
    /// 
    /// # Arguments
    /// 
    /// * `query` - A query string or a (id,query) string tuple or a [`ParsedQuery`].
    /// 
    /// # Returns
    /// 
    /// A stream of blocks received from Clickhouse.
    /// 
    /// # Remarks
    /// 
    /// You probably want [`Connection::query`].
    /// This function will return a stream without 'Unpin' bound, so you may need to pin it before using. see [`futures_util::pin_mut!`]
    pub async fn query_raw<'a>(&'a mut self, query:impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Result<impl Stream<Item = Result<Block>> + Send  + 'a> {
        self.send_query(query).await?;
        self.receive_blocks().map(|stream| ::tokio_stream::StreamExt::filter(stream, | response | match response {
            Ok(block) => block.rows > 0,
            Err(_) => true,
        }))
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`]. The query should be a native query, i.e. it should have a [`format native`](https://clickhouse.com/docs/integrations/data-formats/binary-native) suffix.
    /// * `blocks` - A stream of native blocks to send to Clickhouse. 
    /// 
    /// # Returns
    /// 
    /// A result indicating success or failure of the operation.
    /// 
    /// # Remarks
    ///  
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read and DISCARDED.
    /// You probably want [`Connection::insert`].
    pub async fn insert_raw(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        mut blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> Result<()> {

        self.send_query(query).await?;

        while let Some(block) = blocks.next().await {
            self.send_data(block).await?;
        }

        self.send_empty_block().await?;

        self.discard_blocks().await?;
    
        Ok(())
    }
   
    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`]. The query should be a native query, i.e. it should have a [`format native`](https://clickhouse.com/docs/integrations/data-formats/binary-native) suffix.
    /// * `blocks` - A stream of blocks to send to Clickhouse. Each block should contain rows that implement the [`Row`] trait.
    /// 
    /// # Returns
    /// 
    /// A result indicating success or failure of the operation.
    /// 
    /// # Remarks
    /// 
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read and DISCARDED.
    pub async fn insert<T: Row + Send + Sync + 'static>(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        mut blocks: impl Stream<Item = Vec<T>> + Send + Sync + Unpin + 'static,
    ) -> Result<()> {

        self.send_query(query).await?;
        
        let first_block = self.receive_block().await.ok_or_else(|| {
            KlickhouseError::ProtocolError("missing header block from server".to_string())
        })??;

        while let Some(rows) = blocks.next().await {
            if rows.is_empty() {
                continue;
            }
            let mut block = Block {
                info: BlockInfo::default(),
                rows: rows.len() as u64,
                column_types: first_block.column_types.clone(),
                column_data: IndexMap::new(),
            };
            rows.into_iter()
                .map(|x| x.serialize_row(&first_block.column_types))
                .filter_map(|x| match x {
                    Err(e) => {
                        error!("serialization error during insert (SKIPPED ROWS!): {:?}", e);
                        None
                    }
                    Ok(x) => Some(x),
                })
                .try_for_each(|x| -> Result<()> {
                    for (key, value) in x {
                        let type_ = first_block.column_types.get(&*key).ok_or_else(|| {
                            KlickhouseError::ProtocolError(format!(
                                "missing type for data, column: {key}"
                            ))
                        })?;
                        type_.validate_value(&value)?;
                        if let Some(column) = block.column_data.get_mut(&*key) {
                            column.push(value);
                        } else {
                            block.column_data.insert(key.into_owned(), vec![value]);
                        }
                    }
                    Ok(())
                })?;
            self.send_data(block).await?;
        }

        self.send_empty_block().await?;

        self.discard_blocks().await?;

        Ok(())
    }

    /// Runs a query against Clickhouse, returning a stream of deserialized rows.
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`].
    /// 
    /// # Returns
    /// 
    /// A stream of rows deserialized into the type `T` that implements the [`Row`] trait.
    /// 
    /// # Remarks
    /// 
    /// Note that no rows are returned until Clickhouse sends a full block (but it usually sends more than one block).
    /// This function will return a stream without `Unpin` bound, so you may need to pin it before using. see [`futures_util::pin_mut!`]
    pub async fn query<'a,T: Row + Send + 'a>(
        &'a mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<impl Stream<Item = Result<T>> + Send  + 'a> {
        
        let stream = self.query_raw(query).await?;
        
        Ok(stream.flat_map(|b| match b {
            Ok(mut block) => stream::iter(
                block
                    .take_iter_rows()
                    .filter(|x| !x.is_empty())
                    .map(|m| T::deserialize_row(m))
                    .collect::<Vec<_>>(),
            ),
            Err(e) => stream::iter(vec![Err(e)]),
        }))
    }

    /// Same as [`Connection::insert_raw`], but inserts a single batch of blocks instead of a stream.
    pub async fn insert_vec_raw(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        blocks: Vec<Block>,
    ) -> Result<()> {
        self.send_query(query).await?;

        for block in blocks {
            self.send_data(block).await?;
        }

        self.send_empty_block().await?;

        self.discard_blocks().await?;

        Ok(())
    }

    /// Same as [`Connection::insert`], but inserts a single batch of rows instead of a stream.
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`]. The query should be a native query, i.e. it should have a [`format native`](https://clickhouse.com/docs/integrations/data-formats/binary-native) suffix.
    /// * `rows` - A vector of rows to insert into Clickhouse. Each row should implement the [`Row`] trait.
    /// 
    /// # Returns
    /// 
    /// A result indicating success or failure of the operation.
    pub async fn insert_vec<T: Row + Send + Sync + 'static>(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        rows: Vec<T>,
    ) -> Result<()> {
        
        self.send_query(query).await?;
        
        let first_block = self.receive_block().await.ok_or_else(|| {
            KlickhouseError::ProtocolError("missing header block from server".to_string())
        })??;

        let mut block = Block {
            info: BlockInfo::default(),
            rows: rows.len() as u64,
            column_types: first_block.column_types.clone(),
            column_data: IndexMap::new(),
        };

        rows.into_iter()
            .map(|x| x.serialize_row(&first_block.column_types))
            .filter_map(|x| match x {
                Err(e) => {
                    error!("serialization error during insert (SKIPPED ROWS!): {:?}", e);
                    None
                }
                Ok(x) => Some(x),
            })
            .try_for_each(|x| -> Result<()> {
                for (key, value) in x {
                    let type_ = first_block.column_types.get(&*key).ok_or_else(|| {
                        KlickhouseError::ProtocolError(format!(
                            "missing type for data, column: {key}"
                        ))
                    })?;
                    type_.validate_value(&value)?;
                    if let Some(column) = block.column_data.get_mut(&*key) {
                        column.push(value);
                    } else {
                        block.column_data.insert(key.into_owned(), vec![value]);
                    }
                }
                Ok(())
            })?;
        self.send_data(block).await?;

        self.send_empty_block().await?;

        self.discard_blocks().await?;

        Ok(())
    }


    /// Same as [`Connection::query`], but collects all rows into a `Vec`
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`].
    /// 
    /// # Returns
    /// 
    /// A vector of rows deserialized into the type `T` that implements the [`Row`] trait.
    pub async fn query_vec<'a, T: Row + Send + 'a>(
        &'a mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Vec<T>> {

        let mut result = vec![];

        let stream = self.query::<T>(query).await?;
        
        pin_mut!(stream);

        while let Some(row) = stream.next().await {
            result.push(row?);
        }

        Ok(result)

    }

    /// Same as [`Connection::query`], but returns the first row, and discards the rest.
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`].
    /// 
    /// # Returns
    /// 
    /// An optional row deserialized into the type `T` that implements the [`Row`] trait.
    pub async fn query_first<'a, T: Row + Send + 'a>(
        &'a mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Option<T>> {

        let result =  {
            
            let stream =  self.query::<T>(query).await?;
        
            pin_mut!(stream);
            
            stream.next().await.transpose()

        };

        if let Ok(Some(_)) = &result {
            // If we got a result, we need to discard the rest of the blocks
            // to avoid memory leaks.
            // This is because Clickhouse will send more blocks than we requested.
            // We don't care about the rest of the blocks, so we just discard them.
            self.discard_blocks().await?;
        }

        result

    }

    /// Same as [`Connection::query`], but returns the first row, and discards the rest.
    /// 
    /// # Arguments
    /// 
    /// * `query` -  A query string or a (id,query) string tuple or a [`ParsedQuery`].
    /// 
    /// # Returns
    /// 
    /// An optional tuple of (key, type, value) where key is the first column name, type is the type of the first column, and value is the value of the first column.
    pub async fn execute(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Option<(String,Type,Value)>> {

        self.send_query(query).await?;



         loop {
            if let Some(block_result) = self.receive_block().await {
                let mut block = block_result?;
                if let Some(row) = block.take_iter_rows().next() {
                    if let Some((key, ty, value)) = row.into_iter().next() {
                        self.discard_blocks().await?;
                        break Ok(Some((key.to_owned(), ty.clone(), value)));
                    }
                }
            }
            else {
                break Ok(None);
            }
        }
    }

    /// Receive progress on the queries as they execute.
    pub fn subscribe_progress(&self) -> broadcast::Receiver<Progress> {
        self.progress.subscribe()
    }
}

// Below lines should be uncommented when negative impls are stable
// impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> !Send for Connection<R,W> {}
// impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> !Sync for Connection<R,W> {}

pub type TcpConnection = Connection<BufReader<OwnedReadHalf>, BufWriter<OwnedWriteHalf>>;

/// Connects to a spesific socket address for Clickhouse with additional client info.
/// 
/// # Arguments
/// 
/// * `destination` - A socket address to connect to, can be a string or a tuple of (host, port).
/// * `options` - Client options to use for the connection, such as default database, username, password, etc.
/// * `info` - Client info to use for the connection, such as query kind, initial user, initial query ID, etc.
/// 
/// # Returns
/// 
/// A result containing a [`TcpConnection`] if successful, or an error if the connection fails.
pub async fn connect_with_info<A: ToSocketAddrs>(
    destination: A, 
    options: ClientOptions,
    info: ClientInfo<'static>,
) -> Result<TcpConnection>  {
    let stream = TcpStream::connect(destination).await?;
    stream.set_nodelay(options.tcp_nodelay)?;
    let (read, writer) = stream.into_split();
    let result = Connection::new(BufReader::new(read), BufWriter::new(writer), options, info).await?;
    Ok(result)
}



/// Connects to a spesific socket address for Clickhouse.
/// 
/// # Arguments
/// 
/// * `destination` - A socket address to connect to, can be a string or a tuple of (host, port).
/// * `options` - Client options to use for the connection, such as default database, username, password, etc.
/// 
/// # Returns
/// 
/// A result containing a [`TcpConnection`] if successful, or an error if the connection fails.
pub async fn connect<A: ToSocketAddrs>(
    destination: A, 
    options: ClientOptions,
) -> Result<TcpConnection>  {
    connect_with_info(destination, options, DEFAULT_CLIENT_INFO).await
}

#[cfg(feature = "tls")]
pub type TlsConnection = Connection<BufReader<tokio::io::ReadHalf<::tokio_rustls::client::TlsStream<TcpStream>>>, BufWriter<tokio::io::WriteHalf<::tokio_rustls::client::TlsStream<TcpStream>>>>;

/// Connects to a specific socket address over TLS (rustls) for Clickhouse.
#[cfg(feature = "tls")]
pub async fn connect_tls<A: ToSocketAddrs>(
    destination: A,
    options: ClientOptions,
    name: rustls_pki_types::ServerName<'static>,
    connector: &tokio_rustls::TlsConnector,
) ->  Result<TlsConnection> {
    let stream = TcpStream::connect(destination).await?;
    stream.set_nodelay(options.tcp_nodelay)?;
    let tls_stream = connector.connect(name, stream).await?;
    let (read, writer) = tokio::io::split(tls_stream);
    let result = Connection::new(BufReader::new(read), BufWriter::new(writer), options, DEFAULT_CLIENT_INFO).await?;
    Ok(result)
}

