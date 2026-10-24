use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::env::ModuleEnvironment;
use crate::error::ModuleKitError;
use crate::token_provider::ServiceTokenProvider;
use crate::tokens::ModuleTokenExchangeRequest;

const CONNECTOR_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TOKEN_SAFETY_SECONDS: u64 = 5;
/// Idle host connections a client keeps.
const CONNECTOR_POOL_MAX_IDLE: usize = 8;
/// An idle connection older than this is not reused; well below the host's
/// own idle limit ([`CONNECTOR_FRAMED_IDLE_TIMEOUT`]), so the host does not
/// close a connection just as the kit sends on it.
const CONNECTOR_IDLE_REUSE: Duration = Duration::from_secs(30);

/// Framed, persistent connections to the host's DB connector (step 5.6).
///
/// A connection that starts with this preamble carries frames in both
/// directions: a `u32` big-endian length, then the JSON
/// [`DbConnectorRequest`] or [`DbConnectorResponse`], one request at a time,
/// until either side closes. A connection without it is the legacy one-shot
/// protocol (one JSON request, the client shuts down its write side, one
/// JSON response, the host closes); the messages are the same.
pub const CONNECTOR_FRAMED_PREAMBLE: &[u8] = b"FENRIR-DBC/1\n";
/// Set by a host that accepts framed connections, to
/// [`CONNECTOR_FRAMED_VERSION`]. Without it the kit uses the legacy protocol.
pub const ENV_CONNECTOR_FRAMED: &str = "FENRIR_DB_CONNECTOR_FRAMED";
pub const CONNECTOR_FRAMED_VERSION: &str = "1";
/// Largest request frame the host accepts.
pub const CONNECTOR_MAX_REQUEST_FRAME: usize = 64 * 1024;
/// Largest answer frame the kit accepts.
pub const CONNECTOR_MAX_RESPONSE_FRAME: usize = 256 * 1024 * 1024;
/// The host closes a framed connection after this long without a request.
pub const CONNECTOR_FRAMED_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub enum ConnectorEndpoint {
    #[cfg(unix)]
    Ipc {
        path: String,
    },
    Tcp {
        addr: String,
    },
}

impl ConnectorEndpoint {
    pub fn from_uri(uri: &str) -> Result<Self, ModuleKitError> {
        if let Some(rest) = uri.strip_prefix("ipc://") {
            #[cfg(unix)]
            {
                if rest.trim().is_empty() {
                    return Err(ModuleKitError::InvalidConnectorUri(uri.to_string()));
                }
                return Ok(Self::Ipc {
                    path: rest.trim().to_string(),
                });
            }
            #[cfg(not(unix))]
            {
                return Err(ModuleKitError::InvalidConnectorUri(
                    "ipc protocol is not supported on this platform".into(),
                ));
            }
        }
        if let Some(rest) = uri.strip_prefix("tcp://") {
            if rest.trim().is_empty() {
                return Err(ModuleKitError::InvalidConnectorUri(uri.to_string()));
            }
            return Ok(Self::Tcp {
                addr: rest.trim().to_string(),
            });
        }
        Err(ModuleKitError::InvalidConnectorUri(uri.to_string()))
    }

    fn send(&self, payload: &[u8]) -> Result<Vec<u8>, ModuleKitError> {
        match self {
            #[cfg(unix)]
            ConnectorEndpoint::Ipc { path } => {
                let mut stream = UnixStream::connect(path)?;
                stream.set_read_timeout(Some(CONNECTOR_TIMEOUT)).ok();
                stream.set_write_timeout(Some(CONNECTOR_TIMEOUT)).ok();
                stream.write_all(payload)?;
                stream.shutdown(Shutdown::Write).ok();
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf)?;
                Ok(buf)
            }
            ConnectorEndpoint::Tcp { addr } => {
                let mut stream = TcpStream::connect(addr)?;
                stream.set_read_timeout(Some(CONNECTOR_TIMEOUT)).ok();
                stream.set_write_timeout(Some(CONNECTOR_TIMEOUT)).ok();
                stream.write_all(payload)?;
                stream.shutdown(Shutdown::Write).ok();
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf)?;
                Ok(buf)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConnectorRequest {
    pub token: String,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub intent: Option<DbConnectorIntent>,
    pub command: DbConnectorCommand,
    #[serde(default)]
    pub tenant: Option<DbTenantPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum DbConnectorCommand {
    Simple {
        statement: String,
    },
    Prepared {
        statement: String,
        params: Vec<DbPreparedParam>,
    },
}

impl DbConnectorCommand {
    pub fn statement(&self) -> &str {
        match self {
            DbConnectorCommand::Simple { statement } => statement,
            DbConnectorCommand::Prepared { statement, .. } => statement,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPreparedParam {
    pub name: String,
    pub value: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbTenantPolicy {
    pub param: String,
    #[serde(default)]
    pub mode: DbTenantBindingMode,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbTenantBindingMode {
    #[default]
    Inject,
    RequireMatch,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbConnectorIntent {
    #[default]
    Read,
    Write,
}

impl DbConnectorIntent {
    pub fn requires_write_scope(&self) -> bool {
        matches!(self, DbConnectorIntent::Write)
    }

    /// `Read` only for a plain read; data-modifying CTEs, row locks,
    /// `SELECT … INTO`, side-effect functions and anything unclear are
    /// writes (fail-closed). The host applies the same rule
    /// ([`crate::sql_intent::analyze_statement`]) and never trusts the intent
    /// a client sends.
    pub fn detect(statement: &str) -> Self {
        crate::sql_intent::analyze_statement(statement).intent
    }

    /// The stronger of the caller's intent and what the statement does:
    /// a write stays a write even when the caller asked for a read.
    pub fn at_least(self, other: Self) -> Self {
        if self.requires_write_scope() || other.requires_write_scope() {
            DbConnectorIntent::Write
        } else {
            DbConnectorIntent::Read
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbConnectorResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<Vec<DbConnectorResultView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DbConnectorResponse {
    pub fn ok(results: Vec<DbConnectorResultView>) -> Self {
        Self {
            ok: true,
            results: Some(results),
            error: None,
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            results: None,
            error: Some(message.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbConnectorResultView {
    ResultSet {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    AffectedRows {
        count: u64,
    },
    Command {
        tag: String,
    },
}

/// Client of the host's DB connector.
///
/// - [`Self::execute_async`] (and [`Self::session`]) use async I/O and keep
///   connections to the host open for reuse when the host announces framed
///   connections ([`ENV_CONNECTOR_FRAMED`]); otherwise every call opens a
///   connection with the one-shot legacy protocol, as before.
/// - [`Self::execute`] is the blocking API of earlier kits (one connection
///   per call); it keeps working against every host.
/// - Tokens: a read uses the module token, a write a cached `db:write` token
///   (unchanged). A statement the host will treat as a write (e.g. a
///   data-modifying CTE) gets the write token even when the caller asked
///   for a read.
pub struct DbConnectorClient {
    endpoint: ConnectorEndpoint,
    tokens: Arc<ServiceTokenProvider>,
    cached_write_token: Arc<Mutex<Option<CachedToken>>>,
    framed: bool,
    idle: Mutex<Vec<IdleConnection>>,
    timeout: Duration,
}

impl DbConnectorClient {
    pub fn from_env() -> Result<Self, ModuleKitError> {
        let env = ModuleEnvironment::from_env()?;
        Self::from_environment(env)
    }

    pub fn from_environment(env: ModuleEnvironment) -> Result<Self, ModuleKitError> {
        let tokens = env.token_provider()?;
        let framed = std::env::var(ENV_CONNECTOR_FRAMED)
            .map(|value| value.trim() == CONNECTOR_FRAMED_VERSION)
            .unwrap_or(false);
        Ok(Self::with_parts(env.connector, tokens, framed))
    }

    /// A client for `endpoint`; `framed` only when the host supports framed
    /// connections.
    pub fn with_parts(
        endpoint: ConnectorEndpoint,
        tokens: ServiceTokenProvider,
        framed: bool,
    ) -> Self {
        Self {
            endpoint,
            tokens: Arc::new(tokens),
            cached_write_token: Arc::new(Mutex::new(None)),
            framed,
            idle: Mutex::new(Vec::new()),
            timeout: CONNECTOR_TIMEOUT,
        }
    }

    /// Limit for connecting and for one request/answer exchange
    /// (default 15 s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Whether this client keeps framed connections to the host.
    pub fn is_framed(&self) -> bool {
        self.framed
    }

    /// Blocking call over a fresh connection (legacy protocol).
    pub fn execute(
        &self,
        command: DbConnectorCommand,
        intent: DbConnectorIntent,
        engine: Option<&str>,
        tenant: Option<DbTenantPolicy>,
    ) -> Result<DbConnectorResponse, ModuleKitError> {
        let token_intent = intent.at_least(DbConnectorIntent::detect(command.statement()));
        let token = self.token_for_intent(token_intent)?;
        let request = DbConnectorRequest {
            token,
            engine: engine.map(|e| e.to_string()),
            intent: Some(intent),
            command,
            tenant,
        };
        let payload = serde_json::to_vec(&request)?;
        let response_bytes = self.endpoint.send(&payload)?;
        let response: DbConnectorResponse = serde_json::from_slice(&response_bytes)?;
        Ok(response)
    }

    /// One call with async I/O, on a reused connection when possible.
    pub async fn execute_async(
        &self,
        command: DbConnectorCommand,
        intent: DbConnectorIntent,
        engine: Option<&str>,
        tenant: Option<DbTenantPolicy>,
    ) -> Result<DbConnectorResponse, ModuleKitError> {
        self.session()
            .execute(command, intent, engine, tenant)
            .await
    }

    /// A session holds one host connection for all of its calls (and
    /// returns it to the client's pool when dropped), e.g. for a batch.
    pub fn session(&self) -> ConnectorSession<'_> {
        ConnectorSession {
            client: self,
            conn: None,
        }
    }

    fn token_for_intent(&self, intent: DbConnectorIntent) -> Result<String, ModuleKitError> {
        if intent.requires_write_scope() {
            return fetch_write_token(&self.tokens, &self.cached_write_token);
        }
        self.tokens.current_token()
    }

    /// Like `token_for_intent`; a refresh (blocking HTTP to the control
    /// plane) runs on the blocking pool, never on the async thread.
    async fn token_for_intent_async(
        &self,
        intent: DbConnectorIntent,
    ) -> Result<String, ModuleKitError> {
        if intent.requires_write_scope() {
            if let Some(token) = cached_write_token(&self.cached_write_token) {
                return Ok(token);
            }
            let tokens = Arc::clone(&self.tokens);
            let cache = Arc::clone(&self.cached_write_token);
            return tokio::task::spawn_blocking(move || fetch_write_token(&tokens, &cache))
                .await
                .map_err(|err| {
                    connector_io(
                        std::io::ErrorKind::Other,
                        format!("token task failed: {err}"),
                    )
                })?;
        }
        if let Some(token) = self.tokens.token_if_fresh() {
            return Ok(token);
        }
        let tokens = Arc::clone(&self.tokens);
        tokio::task::spawn_blocking(move || tokens.current_token())
            .await
            .map_err(|err| {
                connector_io(
                    std::io::ErrorKind::Other,
                    format!("token task failed: {err}"),
                )
            })?
    }

    /// An idle connection that still looks alive, if any.
    fn checkout(&self) -> Option<AsyncConnection> {
        let mut idle = self.idle.lock().ok()?;
        while let Some(entry) = idle.pop() {
            if entry.since.elapsed() <= CONNECTOR_IDLE_REUSE && entry.conn.is_alive() {
                return Some(entry.conn);
            }
        }
        None
    }

    fn checkin(&self, conn: AsyncConnection) {
        if let Ok(mut idle) = self.idle.lock() {
            if idle.len() < CONNECTOR_POOL_MAX_IDLE {
                idle.push(IdleConnection {
                    conn,
                    since: Instant::now(),
                });
            }
        }
    }
}

/// One host connection used for a sequence of calls; see
/// [`DbConnectorClient::session`].
pub struct ConnectorSession<'a> {
    client: &'a DbConnectorClient,
    conn: Option<AsyncConnection>,
}

impl ConnectorSession<'_> {
    pub async fn execute(
        &mut self,
        command: DbConnectorCommand,
        intent: DbConnectorIntent,
        engine: Option<&str>,
        tenant: Option<DbTenantPolicy>,
    ) -> Result<DbConnectorResponse, ModuleKitError> {
        let effective = intent.at_least(DbConnectorIntent::detect(command.statement()));
        let token = self.client.token_for_intent_async(effective).await?;
        let request = DbConnectorRequest {
            token,
            engine: engine.map(|e| e.to_string()),
            intent: Some(intent),
            command,
            tenant,
        };
        let payload = serde_json::to_vec(&request)?;
        let bytes = if self.client.framed {
            self.exchange_framed(&payload, !effective.requires_write_scope())
                .await?
        } else {
            exchange_legacy(&self.client.endpoint, &payload, self.client.timeout).await?
        };
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Sends one frame and reads the answer. A reused connection that turns
    /// out dead is replaced once: always when the request could not be
    /// written (the host never saw it), and for reads also when the host
    /// closed before answering. A write is never sent twice otherwise.
    async fn exchange_framed(
        &mut self,
        payload: &[u8],
        read_only: bool,
    ) -> Result<Vec<u8>, ModuleKitError> {
        for attempt in 0..2 {
            let (mut conn, reused) = match self.conn.take().or_else(|| self.client.checkout()) {
                Some(conn) => (conn, true),
                None => (
                    tokio::time::timeout(
                        self.client.timeout,
                        AsyncConnection::connect(&self.client.endpoint),
                    )
                    .await
                    .map_err(|_| {
                        connector_io(std::io::ErrorKind::TimedOut, "connector connect timed out")
                    })??,
                    false,
                ),
            };
            let outcome = tokio::time::timeout(self.client.timeout, conn.exchange(payload)).await;
            match outcome {
                Ok(Ok(response)) => {
                    self.conn = Some(conn);
                    return Ok(response);
                }
                Ok(Err(ExchangeError::NotSent(_))) if reused && attempt == 0 => continue,
                Ok(Err(ExchangeError::NoAnswer(_))) if reused && attempt == 0 && read_only => {
                    continue
                }
                Ok(Err(err)) => return Err(err.into()),
                Err(_) => {
                    return Err(connector_io(
                        std::io::ErrorKind::TimedOut,
                        "connector call timed out",
                    ))
                }
            }
        }
        Err(connector_io(
            std::io::ErrorKind::ConnectionAborted,
            "connector connection lost",
        ))
    }
}

impl Drop for ConnectorSession<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.client.checkin(conn);
        }
    }
}

struct IdleConnection {
    conn: AsyncConnection,
    since: Instant,
}

enum ConnectionStream {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    Tcp(tokio::net::TcpStream),
}

/// A framed connection to the host.
struct AsyncConnection {
    stream: ConnectionStream,
    preamble_sent: bool,
}

enum ExchangeError {
    /// The request did not (completely) reach the host.
    NotSent(std::io::Error),
    /// Written, but the connection ended before the first byte of an answer.
    NoAnswer(std::io::Error),
    /// Anything after that.
    Broken(std::io::Error),
    TooLarge(u64),
}

impl From<ExchangeError> for ModuleKitError {
    fn from(err: ExchangeError) -> Self {
        match err {
            ExchangeError::NotSent(err)
            | ExchangeError::NoAnswer(err)
            | ExchangeError::Broken(err) => ModuleKitError::ConnectorIo(err),
            ExchangeError::TooLarge(len) => connector_io(
                std::io::ErrorKind::InvalidData,
                format!("connector answer too large: {len} bytes"),
            ),
        }
    }
}

impl AsyncConnection {
    async fn connect(endpoint: &ConnectorEndpoint) -> Result<Self, ModuleKitError> {
        let stream = match endpoint {
            #[cfg(unix)]
            ConnectorEndpoint::Ipc { path } => {
                ConnectionStream::Unix(tokio::net::UnixStream::connect(path).await?)
            }
            ConnectorEndpoint::Tcp { addr } => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                stream.set_nodelay(true).ok();
                ConnectionStream::Tcp(stream)
            }
        };
        Ok(Self {
            stream,
            preamble_sent: false,
        })
    }

    /// False when the host closed the connection (or sent something
    /// unasked) while it sat idle.
    fn is_alive(&self) -> bool {
        let mut probe = [0u8; 1];
        let result = match &self.stream {
            #[cfg(unix)]
            ConnectionStream::Unix(stream) => stream.try_read(&mut probe),
            ConnectionStream::Tcp(stream) => stream.try_read(&mut probe),
        };
        matches!(result, Err(err) if err.kind() == std::io::ErrorKind::WouldBlock)
    }

    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match &mut self.stream {
            #[cfg(unix)]
            ConnectionStream::Unix(stream) => stream.write_all(bytes).await,
            ConnectionStream::Tcp(stream) => stream.write_all(bytes).await,
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match &mut self.stream {
            #[cfg(unix)]
            ConnectionStream::Unix(stream) => stream.read(buf).await,
            ConnectionStream::Tcp(stream) => stream.read(buf).await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match &mut self.stream {
            #[cfg(unix)]
            ConnectionStream::Unix(stream) => stream.read_exact(buf).await.map(|_| ()),
            ConnectionStream::Tcp(stream) => stream.read_exact(buf).await.map(|_| ()),
        }
    }

    async fn exchange(&mut self, payload: &[u8]) -> Result<Vec<u8>, ExchangeError> {
        let len = u32::try_from(payload.len()).map_err(|_| {
            ExchangeError::NotSent(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "connector request too large",
            ))
        })?;
        let mut frame = Vec::with_capacity(CONNECTOR_FRAMED_PREAMBLE.len() + 4 + payload.len());
        if !self.preamble_sent {
            frame.extend_from_slice(CONNECTOR_FRAMED_PREAMBLE);
        }
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(payload);
        self.write_all(&frame)
            .await
            .map_err(ExchangeError::NotSent)?;
        self.preamble_sent = true;

        let mut header = [0u8; 4];
        let first = self
            .read(&mut header)
            .await
            .map_err(ExchangeError::NoAnswer)?;
        if first == 0 {
            return Err(ExchangeError::NoAnswer(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connector closed the connection",
            )));
        }
        if first < header.len() {
            self.read_exact(&mut header[first..])
                .await
                .map_err(ExchangeError::Broken)?;
        }
        let answer_len = u32::from_be_bytes(header);
        let answer_len_usize = usize::try_from(answer_len).unwrap_or(usize::MAX);
        if answer_len_usize > CONNECTOR_MAX_RESPONSE_FRAME {
            return Err(ExchangeError::TooLarge(u64::from(answer_len)));
        }
        let mut answer = vec![0u8; answer_len_usize];
        self.read_exact(&mut answer)
            .await
            .map_err(ExchangeError::Broken)?;
        Ok(answer)
    }
}

/// The one-shot protocol of earlier hosts, with async I/O: write the
/// request, shut down the write side, read the answer until EOF.
async fn exchange_legacy(
    endpoint: &ConnectorEndpoint,
    payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, ModuleKitError> {
    let call = async {
        let mut buf = Vec::new();
        match endpoint {
            #[cfg(unix)]
            ConnectorEndpoint::Ipc { path } => {
                let mut stream = tokio::net::UnixStream::connect(path).await?;
                stream.write_all(payload).await?;
                stream.shutdown().await.ok();
                stream.read_to_end(&mut buf).await?;
            }
            ConnectorEndpoint::Tcp { addr } => {
                let mut stream = tokio::net::TcpStream::connect(addr).await?;
                stream.write_all(payload).await?;
                stream.shutdown().await.ok();
                stream.read_to_end(&mut buf).await?;
            }
        }
        Ok::<_, std::io::Error>(buf)
    };
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => Ok(result?),
        Err(_) => Err(connector_io(
            std::io::ErrorKind::TimedOut,
            "connector call timed out",
        )),
    }
}

fn connector_io(kind: std::io::ErrorKind, message: impl Into<String>) -> ModuleKitError {
    ModuleKitError::ConnectorIo(std::io::Error::new(kind, message.into()))
}

fn cached_write_token(cache: &Mutex<Option<CachedToken>>) -> Option<String> {
    let guard = cache.lock().ok()?;
    guard
        .as_ref()
        .filter(|token| token.expires_at > Instant::now())
        .map(|token| token.token.clone())
}

fn fetch_write_token(
    tokens: &ServiceTokenProvider,
    cache: &Mutex<Option<CachedToken>>,
) -> Result<String, ModuleKitError> {
    if let Some(token) = cached_write_token(cache) {
        return Ok(token);
    }
    let response = tokens.issue_scoped_token(ModuleTokenExchangeRequest::db_write())?;
    let ttl = response
        .expires_in_seconds
        .saturating_sub(WRITE_TOKEN_SAFETY_SECONDS);
    let expires_at = Instant::now() + Duration::from_secs(ttl.max(WRITE_TOKEN_SAFETY_SECONDS));
    if let Ok(mut guard) = cache.lock() {
        *guard = Some(CachedToken {
            token: response.token.clone(),
            expires_at,
        });
    }
    Ok(response.token)
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[cfg(test)]
#[path = "connector_tests.rs"]
mod tests;
