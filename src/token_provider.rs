use std::sync::Mutex;

use crate::control_plane::ControlPlaneClient;
use crate::error::ModuleKitError;
use crate::tokens::{ModuleTokenExchangeRequest, ModuleTokenExchangeResponse};
use time::Duration;
use time::OffsetDateTime;

const TOKEN_REFRESH_LEAD_SECS: i64 = 60;

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
    lease: Mutex<ServiceTokenLease>,
    control_plane: Option<ControlPlaneClient>,
    refresh_lead: Duration,
}

impl ServiceTokenProvider {
    pub(crate) fn new(
        initial: ServiceTokenLease,
        control_plane: Option<ControlPlaneClient>,
    ) -> Self {
        Self {
            lease: Mutex::new(initial),
            control_plane,
            refresh_lead: Duration::seconds(TOKEN_REFRESH_LEAD_SECS),
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
        let response = client.exchange_token(
            &bearer,
            ModuleTokenExchangeRequest {
                scopes: Vec::new(),
                reason: Some("service_token_refresh".to_string()),
            },
        )?;
        let mut guard = self.lease.lock().unwrap();
        *guard = ServiceTokenLease::from_exchange(response);
        Ok(())
    }
}
