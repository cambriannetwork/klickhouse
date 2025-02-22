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
    }, io::{ClickhouseRead, ClickhouseWrite}, protocol::{self, CompressionMethod, ServerPacket}, ClientOptions, KlickhouseError, ParsedQuery, Progress, RawRow, Result, Row
};

// Maximum number of progress statuses to keep in memory. New statuses evict old ones.
const PROGRESS_CAPACITY: usize = 100;

pub struct Connection<R: ClickhouseRead, W: ClickhouseWrite> {
    input: InternalClientIn<R>,
    output: InternalClientOut<W>,
    progress: broadcast::Sender<Progress>,
}


/// This is lightweight connection to Clickhouse.
/// It is not thread safe and should not be shared between threads. 
/// It is used to send queries and receive responses.
impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> Connection<R, W> {

    async fn new(reader: R, writer: W, options: ClientOptions) -> Result<Self> {
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
        let query = query.try_into()?.0.trim().to_string();
        let id = Uuid::new_v4();
        self.output
            .send_query(Query {
                id: &id.to_string(),
                info: ClientInfo {
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
                },
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
                Ok(_) => continue,
                Err(e) => return Some(Err(e)),
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

    pub async fn query_raw<'a>(&'a mut self, query:impl TryInto<ParsedQuery, Error = KlickhouseError>) -> Result<impl Stream<Item = Result<Block>> + Send  + 'a> {
        self.send_query(query).await?;
        self.receive_blocks().map(|stream| ::tokio_stream::StreamExt::filter(stream, | response | match response {
            Ok(block) => block.rows > 0,
            Err(_) => true,
        }))
    }


    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read.
    /// You probably want [`Connection::insert_native`].
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

        while let Some(_) = self.receive_block().await {
            // Discard all response blocks
        }
    
        Ok(())
    }
   
    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read and DISCARDED.
    /// Make sure any query you send native data with has a `format native` suffix.
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

        while let Some(_) = self.receive_block().await {
            // Discard all response blocks
        }

        Ok(())
    }

    /// Wrapper over [`Client::insert_native`] to send a single block.
    /// Make sure any query you send native data with has a `format native` suffix.
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


        while let Some(_) = self.receive_block().await {
            // Discard all response blocks
        }

        Ok(())
    }

    /// Runs a query against Clickhouse, returning a stream of deserialized rows.
    /// Note that no rows are returned until Clickhouse sends a full block (but it usually sends more than one block).
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

    /// Same as `query`, but collects all rows into a `Vec`
    pub async fn query_collect<'a, T: Row + Send + 'a>(
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

    /// Same as `query`, but returns the first row and discards the rest.
    pub async fn query_on<'a, T: Row + Send + 'a>(
        &'a mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<T> {

        let stream =  self.query::<T>(query)
        .await?;
        
        pin_mut!(stream);

        stream
        .next()
        .await
        .unwrap_or_else(|| Err(KlickhouseError::MissingRow))
    }

    /// Same as `query`, but returns the first row, if any, and discards the rest.
    pub async fn query_opt<'a, T: Row + Send + 'a>(
        &'a mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Option<T>> {

        let stream =  self.query::<T>(query)
        .await?;
        
        pin_mut!(stream);

        stream.next().await.transpose()
    }

    /// Same as `query`, but discards all returns blocks. Waits until the first block returns from the server to check for errors.
    /// Waiting for the first response block or EOS also prevents the server from aborting the query potentially due to client disconnection.
    pub async fn execute(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<()> {
        let stream = self.query::<RawRow>(query).await?;

        pin_mut!(stream);

        while let Some(next) = stream.next().await {
            next?;
        }
        Ok(())
    }

    /// Same as `execute`, but doesn't wait for a server response. The query could get aborted if the connection is closed quickly.
    pub async fn execute_now(
        &mut self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<()> {
        let _ = self.query::<RawRow>(query).await?;
        Ok(())
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

/// Connects to a spesific socket address for Clickhouse.
pub async fn connect<A: ToSocketAddrs>(
    destination: A, 
    options: ClientOptions,
) -> Result<TcpConnection>  {
    let stream = TcpStream::connect(destination).await?;
    stream.set_nodelay(options.tcp_nodelay)?;
    let (read, writer) = stream.into_split();
    let result = Connection::new(BufReader::new(read), BufWriter::new(writer), options).await?;
    Ok(result)
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
    let result = Connection::new(BufReader::new(read), BufWriter::new(writer), options).await?;
    Ok(result)
}

