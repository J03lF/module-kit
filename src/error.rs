use std::io;

use reqwest::Error as ReqwestError;
use thiserror::Error;
use url::ParseError;

#[derive(Debug, Error)]
pub enum ModuleKitError {
    #[error("environment variable '{0}' missing")]
    MissingEnv(&'static str),
    #[error("environment variable '{name}' invalid: {source}")]
    InvalidEnv {
        name: &'static str,
        source: std::env::VarError,
    },
    #[error("environment variable '{name}' invalid value: {message}")]
    InvalidEnvValue { name: &'static str, message: String },
    #[error("invalid connector URI: {0}")]
    InvalidConnectorUri(String),
    #[error("connector IO error: {0}")]
    ConnectorIo(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("control plane request failed: {0}")]
    Http(#[from] ReqwestError),
    #[error("control plane URL invalid: {0}")]
    ControlPlaneUrl(#[from] ParseError),
    #[error("control plane not configured")]
    ControlPlaneMissing,
    #[error("token exchange rejected: {0}")]
    TokenExchange(String),
    #[error("tls error: {0}")]
    Tls(String),
}

impl ModuleKitError {
    pub fn invalid_env(name: &'static str, source: std::env::VarError) -> Self {
        Self::InvalidEnv { name, source }
    }

    pub fn invalid_env_value(name: &'static str, message: String) -> Self {
        Self::InvalidEnvValue { name, message }
    }
}
