use std::collections::HashMap;
use std::time::Duration;

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::ModuleKitError;

const ENV_GATEWAY_ENDPOINT: &str = "FENRIR_GATEWAY_ENDPOINT";
const DEFAULT_GATEWAY_TIMEOUT: Duration = Duration::from_secs(30);

/// Client for the Fenrir runtime gateway endpoint (`FENRIR_GATEWAY_ENDPOINT`).
#[derive(Clone, Debug)]
pub struct GatewayClient {
    endpoint: String,
    http: Client,
}

impl GatewayClient {
    pub fn new(endpoint: impl Into<String>) -> Result<Self, ModuleKitError> {
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(ModuleKitError::invalid_env_value(
                ENV_GATEWAY_ENDPOINT,
                "value must not be empty".to_string(),
            ));
        }
        let http = Client::builder()
            .timeout(DEFAULT_GATEWAY_TIMEOUT)
            .build()
            .map_err(ModuleKitError::Http)?;
        Ok(Self { endpoint, http })
    }

    pub fn from_env() -> Result<Self, ModuleKitError> {
        match std::env::var(ENV_GATEWAY_ENDPOINT) {
            Ok(value) => Self::new(value.trim().to_string()),
            Err(err) => Err(ModuleKitError::invalid_env(ENV_GATEWAY_ENDPOINT, err)),
        }
    }

    pub async fn call(
        &self,
        request: GatewayRequest,
    ) -> Result<GatewayResponsePayload, ModuleKitError> {
        let response = self.http.post(&self.endpoint).json(&request).send().await?;
        let status = response.status();
        let payload = response.bytes().await?;

        if status.is_success() {
            let parsed: GatewayResponsePayload = serde_json::from_slice(&payload)?;
            return Ok(parsed);
        }

        if let Ok(err) = serde_json::from_slice::<GatewayErrorResponse>(&payload) {
            return Err(ModuleKitError::GatewayRequestFailed {
                status: err.status,
                error: err.error,
            });
        }

        let text = String::from_utf8_lossy(&payload).to_string();
        Err(ModuleKitError::GatewayRequestFailed { status: status.as_u16(), error: text })
    }

    pub async fn call_json<T: DeserializeOwned>(
        &self,
        request: GatewayRequest,
    ) -> Result<GatewayResponse<T>, ModuleKitError> {
        let payload = self.call(request).await?;
        let body = payload.body.into_json()?;
        Ok(GatewayResponse {
            status: payload.status,
            audit_id: payload.audit_id,
            headers: payload.headers,
            body,
        })
    }
}

/// Gateway request payload for `POST FENRIR_GATEWAY_ENDPOINT`.
#[derive(Debug, Serialize, Clone)]
pub struct GatewayRequest {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub query: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl GatewayRequest {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            verb: None,
            path: "/".to_string(),
            body: None,
            headers: HashMap::new(),
            query: HashMap::new(),
            timeout_ms: None,
        }
    }

    pub fn verb(mut self, verb: impl Into<String>) -> Self {
        self.verb = Some(verb.into());
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.insert(key.into(), value.into());
        self
    }

    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn json<T: Serialize>(mut self, value: &T) -> Result<Self, ModuleKitError> {
        self.body = Some(serde_json::to_value(value)?);
        Ok(self)
    }
}

/// Gateway response payload returned by the runtime gateway.
#[derive(Debug, Deserialize)]
pub struct GatewayResponsePayload {
    pub status: u16,
    pub audit_id: String,
    pub headers: HashMap<String, String>,
    pub body: GatewayBody,
}

/// Typed response wrapper for JSON payloads.
#[derive(Debug)]
pub struct GatewayResponse<T> {
    pub status: u16,
    pub audit_id: String,
    pub headers: HashMap<String, String>,
    pub body: T,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format", content = "value")]
pub enum GatewayBody {
    Empty,
    Json(serde_json::Value),
    Text(String),
    Base64(String),
}

impl GatewayBody {
    pub fn into_json<T: DeserializeOwned>(self) -> Result<T, ModuleKitError> {
        match self {
            GatewayBody::Json(value) => serde_json::from_value(value).map_err(ModuleKitError::from),
            GatewayBody::Empty => Err(ModuleKitError::GatewayResponse(
                "gateway response body was empty".to_string(),
            )),
            GatewayBody::Text(_) => Err(ModuleKitError::GatewayResponse(
                "gateway response body was text, expected json".to_string(),
            )),
            GatewayBody::Base64(_) => Err(ModuleKitError::GatewayResponse(
                "gateway response body was base64, expected json".to_string(),
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
struct GatewayErrorResponse {
    pub audit_id: String,
    pub status: u16,
    pub error: String,
}
