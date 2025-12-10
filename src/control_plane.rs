use std::fs;
use std::thread::sleep;
use std::time::Duration;

use reqwest::blocking::Client as BlockingClient;
use reqwest::{Certificate, Identity};
use url::Url;

use crate::env::ControlPlaneEnvironment;
use crate::error::ModuleKitError;
use crate::tokens::{ModuleTokenExchangeRequest, ModuleTokenExchangeResponse};

const TOKEN_ENDPOINT_PATH: &str = "modules/runtime/tokens";

pub(crate) struct ControlPlaneClient {
    token_url: Url,
    http: BlockingClient,
    retries: u32,
    backoff: Duration,
}

impl ControlPlaneClient {
    pub(crate) fn new(env: &ControlPlaneEnvironment) -> Result<Self, ModuleKitError> {
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

    pub(crate) fn exchange_token(
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
                    sleep(delay);
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
