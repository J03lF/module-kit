use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use reqwest::blocking::Client as BlockingClient;
use reqwest::{Certificate, Identity};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use url::Url;

use crate::env::{ControlPlaneEnvironment, ModuleEnvironment};
use crate::error::ModuleKitError;
use crate::tokens::{ModuleTokenExchangeRequest, ModuleTokenExchangeResponse};

const CONNECTOR_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TOKEN_SAFETY_SECONDS: u64 = 5;
const TOKEN_ENDPOINT_PATH: &str = "modules/runtime/tokens";

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbTenantBindingMode {
    Inject,
    RequireMatch,
}

impl Default for DbTenantBindingMode {
    fn default() -> Self {
        Self::Inject
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbConnectorIntent {
    Read,
    Write,
}

impl Default for DbConnectorIntent {
    fn default() -> Self {
        Self::Read
    }
}

impl DbConnectorIntent {
    pub fn requires_write_scope(&self) -> bool {
        matches!(self, DbConnectorIntent::Write)
    }

    pub fn detect(statement: &str) -> Self {
        let keyword = statement
            .trim_start()
            .split_whitespace()
            .next()
            .map(|word| word.to_ascii_lowercase())
            .unwrap_or_default();
        match keyword.as_str() {
            "select" | "show" | "describe" | "with" | "explain" => DbConnectorIntent::Read,
            _ => DbConnectorIntent::Write,
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

pub struct DbConnectorClient {
    endpoint: ConnectorEndpoint,
    default_token: String,
    control_plane: Option<ControlPlaneClient>,
    cached_write_token: Mutex<Option<CachedToken>>,
}

impl DbConnectorClient {
    pub fn from_env() -> Result<Self, ModuleKitError> {
        let env = ModuleEnvironment::from_env()?;
        Self::from_environment(env)
    }

    pub fn from_environment(env: ModuleEnvironment) -> Result<Self, ModuleKitError> {
        let control_plane = match &env.control_plane.url {
            Some(_) => Some(ControlPlaneClient::new(&env.control_plane)?),
            None => None,
        };
        Ok(Self {
            endpoint: env.connector,
            default_token: env.service_token,
            control_plane,
            cached_write_token: Mutex::new(None),
        })
    }

    pub fn execute(
        &self,
        command: DbConnectorCommand,
        intent: DbConnectorIntent,
        engine: Option<&str>,
        tenant: Option<DbTenantPolicy>,
    ) -> Result<DbConnectorResponse, ModuleKitError> {
        let token = self.token_for_intent(intent)?;
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

    fn token_for_intent(&self, intent: DbConnectorIntent) -> Result<String, ModuleKitError> {
        if intent.requires_write_scope() {
            return self.fetch_write_token();
        }
        Ok(self.default_token.clone())
    }

    fn fetch_write_token(&self) -> Result<String, ModuleKitError> {
        if let Some(token) = self.cached_write_token.lock().unwrap().as_ref() {
            if token.expires_at > Instant::now() {
                return Ok(token.token.clone());
            }
        }
        let control = self
            .control_plane
            .as_ref()
            .ok_or(ModuleKitError::ControlPlaneMissing)?;
        let response =
            control.exchange_token(&self.default_token, ModuleTokenExchangeRequest::db_write())?;
        let ttl = response
            .expires_in_seconds
            .saturating_sub(WRITE_TOKEN_SAFETY_SECONDS);
        let expires_at = Instant::now() + Duration::from_secs(ttl.max(WRITE_TOKEN_SAFETY_SECONDS));
        let mut guard = self.cached_write_token.lock().unwrap();
        *guard = Some(CachedToken {
            token: response.token.clone(),
            expires_at,
        });
        Ok(response.token)
    }
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

struct ControlPlaneClient {
    token_url: Url,
    http: BlockingClient,
    retries: u32,
    backoff: Duration,
}

impl ControlPlaneClient {
    fn new(env: &ControlPlaneEnvironment) -> Result<Self, ModuleKitError> {
        let base_url = env
            .url
            .clone()
            .ok_or_else(|| ModuleKitError::ControlPlaneMissing)?;
        let normalized = ensure_trailing_slash(base_url);
        let token_url = normalized
            .join(TOKEN_ENDPOINT_PATH)
            .map_err(ModuleKitError::ControlPlaneUrl)?;
        let mut builder = BlockingClient::builder().timeout(env.timeout);
        if env.tls.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(ca_path) = &env.tls.ca_cert_path {
            let bytes = fs::read(ca_path).map_err(|err| {
                ModuleKitError::Tls(format!("failed to read ca cert {ca_path}: {err}"))
            })?;
            let cert = Certificate::from_pem(&bytes)
                .map_err(|err| ModuleKitError::Tls(format!("invalid ca cert {ca_path}: {err}")))?;
            builder = builder.add_root_certificate(cert);
        }
        if let (Some(cert_path), Some(key_path)) =
            (&env.tls.client_cert_path, &env.tls.client_key_path)
        {
            let mut identity_bytes = fs::read(cert_path).map_err(|err| {
                ModuleKitError::Tls(format!("failed to read client cert {cert_path}: {err}"))
            })?;
            let key_bytes = fs::read(key_path).map_err(|err| {
                ModuleKitError::Tls(format!("failed to read client key {key_path}: {err}"))
            })?;
            identity_bytes.extend_from_slice(&key_bytes);
            let identity = Identity::from_pem(&identity_bytes).map_err(|err| {
                ModuleKitError::Tls(format!(
                    "invalid client identity ({cert_path},{key_path}): {err}"
                ))
            })?;
            builder = builder.identity(identity);
        }
        let client = builder.build()?;
        Ok(Self {
            token_url,
            http: client,
            retries: env.retries,
            backoff: env.backoff,
        })
    }

    fn exchange_token(
        &self,
        bearer: &str,
        request: ModuleTokenExchangeRequest,
    ) -> Result<ModuleTokenExchangeResponse, ModuleKitError> {
        let mut attempts = 0;
        loop {
            match self
                .http
                .post(self.token_url.clone())
                .bearer_auth(bearer)
                .json(&request)
                .send()
            {
                Ok(response) => {
                    return if response.status().is_success() {
                        response.json().map_err(ModuleKitError::from)
                    } else {
                        let text = response.text().unwrap_or_else(|_| "unknown error".into());
                        Err(ModuleKitError::TokenExchange(text))
                    };
                }
                Err(err) => {
                    attempts += 1;
                    if attempts > self.retries {
                        return Err(ModuleKitError::Http(err));
                    }
                    let delay = self.backoff.saturating_mul(attempts);
                    std::thread::sleep(delay);
                }
            }
        }
    }
}

fn ensure_trailing_slash(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let mut path = url.path().to_string();
        if !path.ends_with('/') {
            path.push('/');
        }
        url.set_path(&path);
    }
    url
}
