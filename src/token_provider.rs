use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

use crate::control_plane::{ControlPlaneClient, ExchangeFailure};
use crate::error::ModuleKitError;
use crate::redact::Redacted;
use crate::service::ModuleReportedServices;
use crate::tokens::{ModuleTokenExchangeRequest, ModuleTokenExchangeResponse};
use time::Duration;
use time::OffsetDateTime;

const TOKEN_REFRESH_LEAD_SECS: i64 = 60;
const AUTO_REFRESH_MIN_SLEEP_SECS: u64 = 5;
const AUTO_REFRESH_FALLBACK_SLEEP_SECS: i64 = 300;
const AUTO_REFRESH_RETRY_SECS: u64 = 5;
const AUTO_REFRESH_RETRY_CAP_SECS: u64 = 300;
const AUTO_REFRESH_REASON: &str = "service_token_refresh";

#[derive(Clone)]
pub struct ServiceTokenLease {
    pub token: String,
    pub issued_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
    pub ttl_seconds: Option<u64>,
    captured_at: OffsetDateTime,
}

impl fmt::Debug for ServiceTokenLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceTokenLease")
            .field("token", &Redacted)
            .field("issued_at", &self.issued_at)
            .field("expires_at", &self.expires_at)
            .field("ttl_seconds", &self.ttl_seconds)
            .field("captured_at", &self.captured_at)
            .finish()
    }
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

/// Why refreshing the module's service token failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TokenRefreshFailureKind {
    /// 401/403: the control plane refused the token (revoked, expired past
    /// its grace, host restarted with new keys). Terminal for this lease:
    /// no more refresh requests until [`ServiceTokenProvider::install_lease`].
    Rejected,
    /// Another error status (rate limit, 5xx); retried with backoff.
    Status,
    /// No answer: connect error, timeout, TLS; retried with backoff.
    Transport,
    /// An answer the kit could not read; retried with backoff.
    InvalidResponse,
}

impl TokenRefreshFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Status => "status",
            Self::Transport => "transport",
            Self::InvalidResponse => "invalid_response",
        }
    }

    fn of(failure: &ExchangeFailure) -> Self {
        match failure.status {
            Some(401 | 403) => Self::Rejected,
            Some(status) if !(200..300).contains(&status) => Self::Status,
            Some(_) => Self::InvalidResponse,
            None => Self::Transport,
        }
    }
}

/// One failed refresh, as handed to [`ServiceTokenProvider::set_refresh_hook`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TokenRefreshFailure {
    pub kind: TokenRefreshFailureKind,
    /// HTTP status, when the control plane answered.
    pub status: Option<u16>,
    /// The error text (never the token).
    pub message: String,
    pub consecutive_failures: u32,
    /// When the auto-refresh tries again; `None` once the lease was refused.
    pub retry_in: Option<StdDuration>,
}

/// Refresh state of the module's service token, e.g. for `/health`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TokenRefreshHealth {
    pub last_success: Option<OffsetDateTime>,
    pub last_failure: Option<OffsetDateTime>,
    pub last_failure_kind: Option<TokenRefreshFailureKind>,
    pub last_status: Option<u16>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    /// The control plane refused the current token; only a new one installed
    /// with [`ServiceTokenProvider::install_lease`] recovers.
    pub rejected: bool,
    pub expires_at: Option<OffsetDateTime>,
}

impl TokenRefreshHealth {
    /// The token was not refused and has not expired.
    pub fn is_healthy(&self) -> bool {
        !self.rejected
            && self
                .expires_at
                .is_none_or(|expires| expires > OffsetDateTime::now_utc())
    }
}

type RefreshHook = Arc<dyn Fn(&TokenRefreshFailure) + Send + Sync>;

/// Waits of the auto-refresh; tests shorten them.
#[derive(Debug, Clone, Copy)]
struct RefreshTiming {
    /// Wait before refreshing a lease that is already due.
    due_wait: StdDuration,
    retry_base: StdDuration,
    retry_cap: StdDuration,
}

impl Default for RefreshTiming {
    fn default() -> Self {
        Self {
            due_wait: StdDuration::from_secs(AUTO_REFRESH_MIN_SLEEP_SECS),
            retry_base: StdDuration::from_secs(AUTO_REFRESH_RETRY_SECS),
            retry_cap: StdDuration::from_secs(AUTO_REFRESH_RETRY_CAP_SECS),
        }
    }
}

impl RefreshTiming {
    /// Exponential backoff for the `failures`-th failure in a row, capped,
    /// with jitter in [delay/2, delay] so modules restarted together by the
    /// host do not retry in lockstep.
    fn retry_delay(&self, failures: u32, random: u64) -> StdDuration {
        let doublings = failures.saturating_sub(1).min(16);
        let full = self
            .retry_base
            .saturating_mul(1u32 << doublings)
            .min(self.retry_cap);
        let half = full / 2;
        let span = (full - half).as_millis() as u64;
        half + StdDuration::from_millis(random % (span + 1))
    }
}

fn random_u64() -> u64 {
    // Fresh SipHash keys per call: enough randomness for jitter, no extra crate.
    RandomState::new().build_hasher().finish()
}

#[derive(Default)]
struct RefreshState {
    health: TokenRefreshHealth,
    /// Earliest next attempt after a failure.
    retry_at: Option<Instant>,
    hook: Option<RefreshHook>,
}

/// Lease and refresh state shared by the provider and its refresh thread.
/// Lock order: `lease` before `state`.
struct TokenCore {
    lease: Mutex<ServiceTokenLease>,
    state: Mutex<RefreshState>,
    timing: RefreshTiming,
}

impl TokenCore {
    fn token(&self) -> String {
        self.lease.lock().unwrap().token.clone()
    }

    /// Exchanges `bearer` for a new default token and records the outcome.
    fn refresh(&self, client: &ControlPlaneClient, bearer: String) -> Result<(), ModuleKitError> {
        {
            let state = self.state.lock().unwrap();
            if state.health.rejected {
                return Err(rejected_error(state.health.last_status));
            }
        }
        match client.exchange_token_classified(&bearer, default_refresh_request()) {
            Ok(response) => {
                let mut lease = self.lease.lock().unwrap();
                // A lease installed while this request ran is newer; keep it.
                if lease.token == bearer {
                    *lease = ServiceTokenLease::from_exchange(response);
                }
                let mut state = self.state.lock().unwrap();
                state.health.last_success = Some(OffsetDateTime::now_utc());
                state.health.consecutive_failures = 0;
                state.retry_at = None;
                Ok(())
            }
            Err(failure) => Err(self.record_failure(&bearer, failure)),
        }
    }

    fn record_failure(&self, bearer: &str, failure: ExchangeFailure) -> ModuleKitError {
        let kind = TokenRefreshFailureKind::of(&failure);
        let message = failure.error.to_string();
        let (event, hook) = {
            let lease = self.lease.lock().unwrap();
            if lease.token != bearer {
                // Refused token was replaced meanwhile; its failure is stale.
                return failure.error;
            }
            let mut state = self.state.lock().unwrap();
            let health = &mut state.health;
            health.consecutive_failures = health.consecutive_failures.saturating_add(1);
            health.last_failure = Some(OffsetDateTime::now_utc());
            health.last_failure_kind = Some(kind);
            health.last_status = failure.status;
            health.last_error = Some(message.clone());
            let consecutive_failures = health.consecutive_failures;
            let retry_in = if kind == TokenRefreshFailureKind::Rejected {
                state.health.rejected = true;
                state.retry_at = None;
                None
            } else {
                let delay = self.timing.retry_delay(consecutive_failures, random_u64());
                state.retry_at = Some(Instant::now() + delay);
                Some(delay)
            };
            let event = TokenRefreshFailure {
                kind,
                status: failure.status,
                message,
                consecutive_failures,
                retry_in,
            };
            (event, state.hook.clone())
        };
        // Outside the locks: the hook may call back into the provider.
        if let Some(hook) = hook {
            hook(&event);
        }
        failure.error
    }

    /// How long the refresh thread sleeps; `None` while the lease is refused.
    fn next_wait(&self, refresh_lead: Duration) -> Option<StdDuration> {
        {
            let state = self.state.lock().unwrap();
            if state.health.rejected {
                return None;
            }
            if let Some(retry_at) = state.retry_at {
                return Some(retry_at.saturating_duration_since(Instant::now()));
            }
        }
        let fallback = Duration::seconds(AUTO_REFRESH_FALLBACK_SLEEP_SECS);
        let lease = self.lease.lock().unwrap();
        let wait = match lease.remaining_duration() {
            Some(remaining) if remaining <= refresh_lead => return Some(self.timing.due_wait),
            Some(remaining) => (remaining - refresh_lead).min(fallback),
            None => fallback,
        };
        Some(duration_to_std(wait))
    }

    fn install(&self, new_lease: ServiceTokenLease) {
        let mut lease = self.lease.lock().unwrap();
        *lease = new_lease;
        let mut state = self.state.lock().unwrap();
        state.health.rejected = false;
        state.health.consecutive_failures = 0;
        state.retry_at = None;
    }

    fn health(&self) -> TokenRefreshHealth {
        let expires_at = self.lease.lock().unwrap().effective_expires_at();
        let mut health = self.state.lock().unwrap().health.clone();
        health.expires_at = expires_at;
        health
    }
}

pub struct ServiceTokenProvider {
    core: Arc<TokenCore>,
    control_plane: Option<Arc<ControlPlaneClient>>,
    refresh_lead: Duration,
    _auto_refresh: Option<AutoRefreshHandle>,
}

impl ServiceTokenProvider {
    pub(crate) fn new(
        initial: ServiceTokenLease,
        control_plane: Option<ControlPlaneClient>,
    ) -> Self {
        Self::with_timing(initial, control_plane, RefreshTiming::default())
    }

    fn with_timing(
        initial: ServiceTokenLease,
        control_plane: Option<ControlPlaneClient>,
        timing: RefreshTiming,
    ) -> Self {
        let core = Arc::new(TokenCore {
            lease: Mutex::new(initial),
            state: Mutex::new(RefreshState::default()),
            timing,
        });
        let control_plane = control_plane.map(Arc::new);
        let refresh_lead = Duration::seconds(TOKEN_REFRESH_LEAD_SECS);
        let auto_refresh = control_plane.as_ref().map(|client| {
            AutoRefreshHandle::start(Arc::clone(&core), Arc::clone(client), refresh_lead)
        });
        Self {
            core,
            control_plane,
            refresh_lead,
            _auto_refresh: auto_refresh,
        }
    }

    pub fn current_token(&self) -> Result<String, ModuleKitError> {
        if self.control_plane.is_none() {
            return Ok(self.core.token());
        }
        let refresh_token = {
            let lease = self.core.lease.lock().unwrap();
            if lease.should_refresh(self.refresh_lead) {
                Some(lease.token.clone())
            } else {
                return Ok(lease.token.clone());
            }
        };
        if let Some(bearer) = refresh_token {
            self.refresh_default_token(bearer)?;
        }
        Ok(self.core.token())
    }

    /// The current token when it needs no refresh, without blocking; `None`
    /// when [`Self::current_token`] would have to refresh it first.
    pub fn token_if_fresh(&self) -> Option<String> {
        let lease = self.core.lease.lock().ok()?;
        if self.control_plane.is_none() || !lease.should_refresh(self.refresh_lead) {
            Some(lease.token.clone())
        } else {
            None
        }
    }

    /// Refresh state of the module token (last success, failures in a row,
    /// whether the control plane refused it), e.g. for a module's `/health`.
    pub fn refresh_health(&self) -> TokenRefreshHealth {
        self.core.health()
    }

    /// Called on every failed refresh of the module token (auto-refresh and
    /// on-demand), e.g. to log it. The kit itself prints nothing.
    pub fn set_refresh_hook<F>(&self, hook: F)
    where
        F: Fn(&TokenRefreshFailure) + Send + Sync + 'static,
    {
        self.core.state.lock().unwrap().hook = Some(Arc::new(hook));
    }

    /// Replaces the module token, e.g. after the control plane refused the
    /// old one; clears the refused state and resumes the auto-refresh.
    pub fn install_lease(&self, lease: ServiceTokenLease) {
        self.core.install(lease);
        if let Some(handle) = &self._auto_refresh {
            handle.wake();
        }
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

    pub fn publish_services(
        &self,
        payload: &mut ModuleReportedServices,
    ) -> Result<(), ModuleKitError> {
        let bearer = self.current_token()?;
        let client = self
            .control_plane
            .as_ref()
            .ok_or(ModuleKitError::ControlPlaneMissing)?;
        payload.sign_with_token(&bearer)?;
        client.publish_services(&bearer, payload)
    }

    fn refresh_default_token(&self, bearer: String) -> Result<(), ModuleKitError> {
        let client = self
            .control_plane
            .as_ref()
            .ok_or(ModuleKitError::ControlPlaneMissing)?;
        self.core.refresh(client, bearer)
    }
}

struct AutoRefreshHandle {
    shutdown: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl AutoRefreshHandle {
    fn start(
        core: Arc<TokenCore>,
        client: Arc<ControlPlaneClient>,
        refresh_lead: Duration,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            run_auto_refresh_loop(core, client, refresh_lead, thread_shutdown);
        });
        Self {
            shutdown,
            thread: Some(handle),
        }
    }

    fn wake(&self) {
        if let Some(handle) = &self.thread {
            handle.thread().unpark();
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
    core: Arc<TokenCore>,
    client: Arc<ControlPlaneClient>,
    refresh_lead: Duration,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let Some(wait) = core.next_wait(refresh_lead) else {
            // Refused lease: send nothing until a new one is installed
            // (which unparks this thread).
            thread::park_timeout(core.timing.retry_cap);
            continue;
        };
        if !wait.is_zero() {
            let started = Instant::now();
            thread::park_timeout(wait);
            // Woken early (new lease, shutdown, spurious): work the wait out again.
            if started.elapsed() < wait {
                continue;
            }
        }
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        // Failures land in the health state and the hook.
        let _ = core.refresh(&client, core.token());
    }
}

fn duration_to_std(duration: Duration) -> StdDuration {
    if duration.is_negative() {
        StdDuration::from_secs(0)
    } else {
        StdDuration::from_secs(duration.whole_seconds() as u64)
    }
}

fn default_refresh_request() -> ModuleTokenExchangeRequest {
    ModuleTokenExchangeRequest {
        scopes: Vec::new(),
        reason: Some(AUTO_REFRESH_REASON.to_string()),
    }
}

fn rejected_error(status: Option<u16>) -> ModuleKitError {
    let status = status.map(|s| s.to_string()).unwrap_or_else(|| "?".into());
    ModuleKitError::TokenExchange(format!(
        "service token refused by the control plane (status {status}); \
         not retried until a new token is installed"
    ))
}

#[cfg(test)]
#[path = "token_provider_tests.rs"]
mod tests;
