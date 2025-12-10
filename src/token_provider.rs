use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration as StdDuration;

use crate::control_plane::ControlPlaneClient;
use crate::error::ModuleKitError;
use crate::tokens::{ModuleTokenExchangeRequest, ModuleTokenExchangeResponse};
use time::Duration;
use time::OffsetDateTime;

const TOKEN_REFRESH_LEAD_SECS: i64 = 60;
const AUTO_REFRESH_MIN_SLEEP_SECS: i64 = 5;
const AUTO_REFRESH_FALLBACK_SLEEP_SECS: i64 = 300;
const AUTO_REFRESH_RETRY_SECS: u64 = 5;
const AUTO_REFRESH_REASON: &str = "service_token_refresh";

#[derive(Debug, Clone)]
pub struct ServiceTokenLease {
    pub token: String,
    pub issued_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
    pub ttl_seconds: Option<u64>,
    captured_at: OffsetDateTime,
}

impl ServiceTokenLease {
    pub fn new(
        token: impl Into<String>,
        issued_at: Option<OffsetDateTime>,
        expires_at: Option<OffsetDateTime>,
        ttl_seconds: Option<u64>,
    ) -> Self {
        Self {
            token: token.into(),
            issued_at,
            expires_at,
            ttl_seconds,
            captured_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn from_exchange(response: ModuleTokenExchangeResponse) -> Self {
        let now = OffsetDateTime::now_utc();
        let expires_at = now + Duration::seconds(response.expires_in_seconds as i64);
        Self {
            token: response.token,
            issued_at: Some(now),
            expires_at: Some(expires_at),
            ttl_seconds: Some(response.expires_in_seconds),
            captured_at: now,
        }
    }

    fn effective_expires_at(&self) -> Option<OffsetDateTime> {
        if let Some(expires_at) = self.expires_at {
            return Some(expires_at);
        }
        self.ttl_seconds
            .map(|ttl| self.captured_at + Duration::seconds(ttl as i64))
    }

    fn remaining_duration(&self) -> Option<Duration> {
        let now = OffsetDateTime::now_utc();
        self.effective_expires_at()
            .map(|expires| (expires - now).max(Duration::ZERO))
    }

    pub fn should_refresh(&self, lead: Duration) -> bool {
        self.remaining_duration()
            .map(|remaining| remaining <= lead)
            .unwrap_or(false)
    }
}

pub struct ServiceTokenProvider {
    lease: Arc<Mutex<ServiceTokenLease>>,
    control_plane: Option<Arc<ControlPlaneClient>>,
    refresh_lead: Duration,
    _auto_refresh: Option<AutoRefreshHandle>,
}

impl ServiceTokenProvider {
    pub(crate) fn new(
        initial: ServiceTokenLease,
        control_plane: Option<ControlPlaneClient>,
    ) -> Self {
        let lease = Arc::new(Mutex::new(initial));
        let control_plane = control_plane.map(|client| Arc::new(client));
        let refresh_lead = Duration::seconds(TOKEN_REFRESH_LEAD_SECS);
        let auto_refresh = control_plane
            .as_ref()
            .map(|client| AutoRefreshHandle::start(Arc::clone(&lease), Arc::clone(client), refresh_lead));
        Self {
            lease,
            control_plane,
            refresh_lead,
            _auto_refresh: auto_refresh,
        }
    }

    pub fn current_token(&self) -> Result<String, ModuleKitError> {
        if self.control_plane.is_none() {
            return Ok(self.lease.lock().unwrap().token.clone());
        }
        let refresh_token = {
            let lease = self.lease.lock().unwrap();
            if lease.should_refresh(self.refresh_lead) {
                Some(lease.token.clone())
            } else {
                return Ok(lease.token.clone());
            }
        };
        if let Some(bearer) = refresh_token {
            self.refresh_default_token(bearer)?;
        }
        Ok(self.lease.lock().unwrap().token.clone())
    }

    pub fn issue_scoped_token(
        &self,
        request: ModuleTokenExchangeRequest,
    ) -> Result<ModuleTokenExchangeResponse, ModuleKitError> {
        let bearer = self.current_token()?;
        let client = self
            .control_plane
            .as_ref()
            .ok_or(ModuleKitError::ControlPlaneMissing)?;
        client.exchange_token(&bearer, request)
    }

    fn refresh_default_token(&self, bearer: String) -> Result<(), ModuleKitError> {
        let client = self
            .control_plane
            .as_ref()
            .ok_or(ModuleKitError::ControlPlaneMissing)?;
        exchange_default_token(&self.lease, client, bearer)
    }
}

struct AutoRefreshHandle {
    shutdown: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl AutoRefreshHandle {
    fn start(
        lease: Arc<Mutex<ServiceTokenLease>>,
        client: Arc<ControlPlaneClient>,
        refresh_lead: Duration,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            run_auto_refresh_loop(lease, client, refresh_lead, thread_shutdown);
        });
        Self {
            shutdown,
            thread: Some(handle),
        }
    }
}

impl Drop for AutoRefreshHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}

fn run_auto_refresh_loop(
    lease: Arc<Mutex<ServiceTokenLease>>,
    client: Arc<ControlPlaneClient>,
    refresh_lead: Duration,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let wait = next_refresh_wait(&lease, refresh_lead);
        if wait.is_zero() {
            // no wait, continue to refresh immediately
        } else {
            thread::park_timeout(wait);
        }
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let bearer = { lease.lock().unwrap().token.clone() };
        match client.exchange_token(
            &bearer,
            ModuleTokenExchangeRequest {
                scopes: Vec::new(),
                reason: Some(AUTO_REFRESH_REASON.to_string()),
            },
        ) {
            Ok(response) => {
                let mut guard = lease.lock().unwrap();
                *guard = ServiceTokenLease::from_exchange(response);
            }
            Err(_) => {
                thread::sleep(StdDuration::from_secs(AUTO_REFRESH_RETRY_SECS));
            }
        }
    }
}

fn next_refresh_wait(lease: &Arc<Mutex<ServiceTokenLease>>, refresh_lead: Duration) -> StdDuration {
    let fallback = Duration::seconds(AUTO_REFRESH_FALLBACK_SLEEP_SECS);
    let wait_duration = {
        let guard = lease.lock().unwrap();
        match guard.remaining_duration() {
            Some(remaining) => {
                if remaining <= refresh_lead {
                    Duration::seconds(AUTO_REFRESH_MIN_SLEEP_SECS)
                } else {
                    remaining - refresh_lead
                }
            }
            None => fallback,
        }
    };
    duration_to_std(wait_duration)
}

fn duration_to_std(duration: Duration) -> StdDuration {
    if duration.is_negative() {
        StdDuration::from_secs(0)
    } else {
        StdDuration::from_secs(duration.whole_seconds() as u64)
    }
}

fn exchange_default_token(
    lease: &Arc<Mutex<ServiceTokenLease>>,
    client: &Arc<ControlPlaneClient>,
    bearer: String,
) -> Result<(), ModuleKitError> {
    let response = client.exchange_token(
        &bearer,
        ModuleTokenExchangeRequest {
            scopes: Vec::new(),
            reason: Some(AUTO_REFRESH_REASON.to_string()),
        },
    )?;
    let mut guard = lease.lock().unwrap();
    *guard = ServiceTokenLease::from_exchange(response);
    Ok(())
}
