use std::fs;
use std::thread::sleep;
use std::time::Duration;

use reqwest::blocking::Client as BlockingClient;
use reqwest::{Certificate, Identity};
use url::Url;

use crate::env::ControlPlaneEnvironment;
use crate::error::ModuleKitError;
use crate::service::ModuleReportedServices;
use crate::tokens::{ModuleTokenExchangeRequest, ModuleTokenExchangeResponse};

const TOKEN_ENDPOINT_PATH: &str = "modules/runtime/tokens";
const SERVICES_ENDPOINT_PATH: &str = "modules/runtime/services";

#[derive(Clone)]
pub(crate) struct ControlPlaneClient {
    token_url: Url,
    services_url: Url,
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
        let services_url = normalized
            .join(SERVICES_ENDPOINT_PATH)
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
            services_url,
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

    pub(crate) fn publish_services(
        &self,
        bearer: &str,
        payload: &ModuleReportedServices,
    ) -> Result<(), ModuleKitError> {
        let mut attempts = 0;
        loop {
            match self
                .http
                .post(self.services_url.clone())
                .bearer_auth(bearer)
                .json(payload)
                .send()
            {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    }
                    let text = response.text().unwrap_or_else(|_| "unknown error".into());
                    return Err(ModuleKitError::ServicePublish(text));
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

#[cfg(test)]
mod tests {
    //! TLS setup of the control-plane client against reqwest 0.12 + rustls.
    //! Key material is generated per run with the `openssl` CLI into a temp
    //! dir (nothing is checked in); the tests skip when `openssl` is missing.
    use super::*;
    use crate::env::ControlPlaneTlsEnvironment;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    struct TempDir(PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn temp_dir(tag: &str) -> TempDir {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir = std::env::temp_dir().join(format!(
            "module-kit-cp-{tag}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    fn openssl(args: &[&str]) -> bool {
        Command::new("openssl")
            .args(args)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Self-signed cert + PKCS#8 key (`algo`: "ec" or "rsa").
    fn cert_and_key(dir: &Path, name: &str, algo: &str) -> Option<(String, String)> {
        let key = dir.join(format!("{name}.key"));
        let cert = dir.join(format!("{name}.crt"));
        let (key_s, cert_s) = (key.to_str()?, cert.to_str()?);
        let keyed = match algo {
            "ec" => openssl(&[
                "genpkey",
                "-algorithm",
                "EC",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-out",
                key_s,
            ]),
            _ => openssl(&[
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
                key_s,
            ]),
        };
        if !keyed {
            return None;
        }
        if !openssl(&[
            "req",
            "-new",
            "-x509",
            "-key",
            key_s,
            "-out",
            cert_s,
            "-days",
            "1",
            "-subj",
            "/CN=module-kit-test",
        ]) {
            return None;
        }
        Some((cert_s.to_string(), key_s.to_string()))
    }

    fn env(tls: ControlPlaneTlsEnvironment) -> ControlPlaneEnvironment {
        ControlPlaneEnvironment {
            url: Some(Url::parse("https://127.0.0.1:1/").unwrap()),
            timeout: Duration::from_millis(500),
            retries: 0,
            backoff: Duration::from_millis(1),
            tls,
        }
    }

    #[test]
    fn plain_client_builds_and_joins_endpoints() {
        let mut e = env(ControlPlaneTlsEnvironment::default());
        e.url = Some(Url::parse("http://127.0.0.1:8080/base").unwrap());
        let client = ControlPlaneClient::new(&e).unwrap();
        assert_eq!(
            client.token_url.as_str(),
            "http://127.0.0.1:8080/base/modules/runtime/tokens"
        );
        assert_eq!(
            client.services_url.as_str(),
            "http://127.0.0.1:8080/base/modules/runtime/services"
        );
    }

    #[test]
    fn missing_url_is_an_error() {
        let mut e = env(ControlPlaneTlsEnvironment::default());
        e.url = None;
        assert!(matches!(
            ControlPlaneClient::new(&e),
            Err(ModuleKitError::ControlPlaneMissing)
        ));
    }

    #[test]
    fn ca_cert_and_client_identity_load_for_ec_and_rsa_pkcs8_keys() {
        let dir = temp_dir("ok");
        for algo in ["ec", "rsa"] {
            let Some((cert, key)) = cert_and_key(&dir.0, algo, algo) else {
                eprintln!("openssl unavailable, skipping");
                return;
            };
            let tls = ControlPlaneTlsEnvironment {
                ca_cert_path: Some(cert.clone()),
                client_cert_path: Some(cert),
                client_key_path: Some(key),
                accept_invalid_certs: false,
            };
            if let Err(err) = ControlPlaneClient::new(&env(tls)) {
                panic!("{algo}: client with CA + identity failed: {err}");
            }
        }
    }

    /// Pins today's behaviour (same with reqwest 0.11 and 0.12): cert and key
    /// are concatenated as-is, so a cert file without a trailing newline
    /// yields `...CERTIFICATE----------BEGIN PRIVATE KEY...` and is rejected
    /// at startup (fail-closed). Inserting a newline between the two would
    /// accept it; tracked as a follow-up, not changed here.
    #[test]
    fn cert_file_without_trailing_newline_is_rejected() {
        let dir = temp_dir("nl");
        let Some((cert, key)) = cert_and_key(&dir.0, "nl", "ec") else {
            eprintln!("openssl unavailable, skipping");
            return;
        };
        let trimmed = fs::read_to_string(&cert).unwrap().trim_end().to_string();
        fs::write(&cert, trimmed).unwrap();
        let tls = ControlPlaneTlsEnvironment {
            client_cert_path: Some(cert),
            client_key_path: Some(key),
            ..Default::default()
        };
        assert!(matches!(
            ControlPlaneClient::new(&env(tls)),
            Err(ModuleKitError::Tls(_))
        ));
    }

    /// Pins today's behaviour (same with reqwest 0.11 and 0.12): a CA file
    /// without any PEM certificate adds no root and the client still builds,
    /// so it trusts only the built-in webpki roots. A private-CA control plane
    /// then fails the handshake instead of being trusted (fail-closed), but
    /// the misconfiguration only shows at the first request. An invalid
    /// client identity, by contrast, is a startup error.
    #[test]
    fn garbage_ca_is_ignored_but_garbage_identity_is_an_error() {
        let dir = temp_dir("bad");
        let bad = dir.0.join("bad.pem");
        fs::write(&bad, "not a certificate").unwrap();
        let bad = bad.to_str().unwrap().to_string();
        let ca = ControlPlaneTlsEnvironment {
            ca_cert_path: Some(bad.clone()),
            ..Default::default()
        };
        assert!(ControlPlaneClient::new(&env(ca)).is_ok());
        let identity = ControlPlaneTlsEnvironment {
            client_cert_path: Some(bad.clone()),
            client_key_path: Some(bad),
            ..Default::default()
        };
        assert!(matches!(
            ControlPlaneClient::new(&env(identity)),
            Err(ModuleKitError::Tls(_))
        ));
    }

    #[test]
    fn identity_without_a_private_key_is_a_tls_error() {
        let dir = temp_dir("nokey");
        let Some((cert, _key)) = cert_and_key(&dir.0, "nokey", "ec") else {
            eprintln!("openssl unavailable, skipping");
            return;
        };
        let tls = ControlPlaneTlsEnvironment {
            client_cert_path: Some(cert.clone()),
            client_key_path: Some(cert),
            ..Default::default()
        };
        assert!(matches!(
            ControlPlaneClient::new(&env(tls)),
            Err(ModuleKitError::Tls(_))
        ));
    }

    #[test]
    fn timeout_applies_to_requests() {
        // A listener that accepts but never answers: the request must give up
        // after the configured timeout instead of hanging.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let _hold = std::thread::spawn(move || {
            let _conn = listener.accept();
            std::thread::sleep(Duration::from_secs(5));
        });
        let mut e = env(ControlPlaneTlsEnvironment::default());
        e.url = Some(Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap());
        e.timeout = Duration::from_millis(300);
        let client = ControlPlaneClient::new(&e).unwrap();
        let started = std::time::Instant::now();
        let result = client.publish_services(
            "t",
            &serde_json::from_value::<ModuleReportedServices>(
                serde_json::json!({ "module_id": "t", "services": [] }),
            )
            .unwrap(),
        );
        assert!(result.is_err());
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
    }
}
