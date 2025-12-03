use std::env;
use std::env::VarError;
use std::time::Duration;

use url::Url;

use crate::connector::ConnectorEndpoint;
use crate::error::ModuleKitError;

const ENV_MODULE_ID: &str = "FENRIR_MODULE_ID";
const ENV_SERVICE_ID: &str = "FENRIR_SERVICE_ID";
const ENV_SERVICE_TOKEN: &str = "FENRIR_SERVICE_TOKEN";
const ENV_CONNECTOR_URI: &str = "FENRIR_DB_CONNECTOR_URI";
const ENV_CONNECTOR_PROTOCOL: &str = "FENRIR_DB_CONNECTOR_PROTOCOL";
const ENV_CONNECTOR_ENDPOINT: &str = "FENRIR_DB_CONNECTOR_ENDPOINT";
const ENV_CONTROL_PLANE_URL: &str = "FENRIR_CONTROL_PLANE_URL";
const ENV_CONTROL_PLANE_TIMEOUT_MS: &str = "FENRIR_CONTROL_PLANE_TIMEOUT_MS";
const ENV_CONTROL_PLANE_RETRY_ATTEMPTS: &str = "FENRIR_CONTROL_PLANE_RETRY_ATTEMPTS";
const ENV_CONTROL_PLANE_RETRY_BACKOFF_MS: &str = "FENRIR_CONTROL_PLANE_RETRY_BACKOFF_MS";
const ENV_CONTROL_PLANE_TLS_CA_CERT: &str = "FENRIR_CONTROL_PLANE_TLS_CA_CERT";
const ENV_CONTROL_PLANE_TLS_CLIENT_CERT: &str = "FENRIR_CONTROL_PLANE_TLS_CLIENT_CERT";
const ENV_CONTROL_PLANE_TLS_CLIENT_KEY: &str = "FENRIR_CONTROL_PLANE_TLS_CLIENT_KEY";
const ENV_CONTROL_PLANE_TLS_ACCEPT_INVALID: &str = "FENRIR_CONTROL_PLANE_TLS_ACCEPT_INVALID";

fn read_env(name: &'static str) -> Result<String, ModuleKitError> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(err) => match err {
            VarError::NotPresent => Err(ModuleKitError::MissingEnv(name)),
            _ => Err(ModuleKitError::invalid_env(name, err)),
        },
    }
}

fn optional_env(name: &'static str) -> Result<Option<String>, ModuleKitError> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) => Ok(None),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(ModuleKitError::invalid_env(name, err)),
    }
}

fn read_u64_env(name: &'static str, default: u64) -> Result<u64, ModuleKitError> {
    match env::var(name) {
        Ok(value) => value.trim().parse::<u64>().map_err(|_| {
            ModuleKitError::invalid_env_value(name, format!("expected integer, got '{value}'"))
        }),
        Err(VarError::NotPresent) => Ok(default),
        Err(err) => Err(ModuleKitError::invalid_env(name, err)),
    }
}

fn read_u32_env(name: &'static str, default: u32) -> Result<u32, ModuleKitError> {
    let value = read_u64_env(name, default as u64)?;
    u32::try_from(value).map_err(|_| {
        ModuleKitError::invalid_env_value(name, format!("value '{value}' exceeds u32::MAX"))
    })
}

fn read_bool_env(name: &'static str, default: bool) -> Result<bool, ModuleKitError> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            other if other.is_empty() => Ok(default),
            other => Err(ModuleKitError::invalid_env_value(
                name,
                format!("expected boolean, got '{other}'"),
            )),
        },
        Err(VarError::NotPresent) => Ok(default),
        Err(err) => Err(ModuleKitError::invalid_env(name, err)),
    }
}

#[derive(Debug, Clone)]
pub struct ModuleEnvironment {
    pub module_id: String,
    pub service_id: String,
    pub service_token: String,
    pub connector: ConnectorEndpoint,
    pub control_plane: ControlPlaneEnvironment,
}

impl ModuleEnvironment {
    pub fn from_env() -> Result<Self, ModuleKitError> {
        let module_id = read_env(ENV_MODULE_ID)?;
        let service_id = read_env(ENV_SERVICE_ID)?;
        let service_token = read_env(ENV_SERVICE_TOKEN)?;
        let connector_uri = match optional_env(ENV_CONNECTOR_URI)? {
            Some(uri) => uri,
            None => {
                let protocol = read_env(ENV_CONNECTOR_PROTOCOL)?;
                let endpoint = read_env(ENV_CONNECTOR_ENDPOINT)?;
                format!("{protocol}://{endpoint}")
            }
        };
        let connector = ConnectorEndpoint::from_uri(&connector_uri)?;
        let control_plane_url = optional_env(ENV_CONTROL_PLANE_URL)?
            .map(|value| Url::parse(value.trim()))
            .transpose()?;
        let control_plane = ControlPlaneEnvironment::from_env(control_plane_url)?;
        Ok(Self {
            module_id,
            service_id,
            service_token,
            connector,
            control_plane,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneEnvironment {
    pub url: Option<Url>,
    pub timeout: Duration,
    pub retries: u32,
    pub backoff: Duration,
    pub tls: ControlPlaneTlsEnvironment,
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneTlsEnvironment {
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub accept_invalid_certs: bool,
}

impl ControlPlaneEnvironment {
    fn from_env(url: Option<Url>) -> Result<Self, ModuleKitError> {
        Ok(Self {
            url,
            timeout: Duration::from_millis(read_u64_env(ENV_CONTROL_PLANE_TIMEOUT_MS, 10_000)?),
            retries: read_u32_env(ENV_CONTROL_PLANE_RETRY_ATTEMPTS, 2)?,
            backoff: Duration::from_millis(read_u64_env(ENV_CONTROL_PLANE_RETRY_BACKOFF_MS, 200)?),
            tls: ControlPlaneTlsEnvironment::from_env()?,
        })
    }
}

impl ControlPlaneTlsEnvironment {
    fn from_env() -> Result<Self, ModuleKitError> {
        Ok(Self {
            ca_cert_path: optional_env(ENV_CONTROL_PLANE_TLS_CA_CERT)?,
            client_cert_path: optional_env(ENV_CONTROL_PLANE_TLS_CLIENT_CERT)?,
            client_key_path: optional_env(ENV_CONTROL_PLANE_TLS_CLIENT_KEY)?,
            accept_invalid_certs: read_bool_env(ENV_CONTROL_PLANE_TLS_ACCEPT_INVALID, false)?,
        })
    }
}
