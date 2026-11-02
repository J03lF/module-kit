use std::fmt;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleTokenExchangeRequest {
    pub scopes: Vec<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl ModuleTokenExchangeRequest {
    pub fn db_write() -> Self {
        Self {
            scopes: vec!["db:write".to_string()],
            reason: Some("db_connector".to_string()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ModuleTokenExchangeResponse {
    pub token: String,
    pub scopes: Vec<String>,
    pub expires_in_seconds: u64,
}

impl fmt::Debug for ModuleTokenExchangeResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModuleTokenExchangeResponse")
            .field("token", &Redacted)
            .field("scopes", &self.scopes)
            .field("expires_in_seconds", &self.expires_in_seconds)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_of_an_exchange_response_hides_the_token() {
        let response = ModuleTokenExchangeResponse {
            token: "tok-secret-4711".to_string(),
            scopes: vec!["db:write".to_string()],
            expires_in_seconds: 60,
        };
        let shown = format!("{response:?} {response:#?}");
        assert!(!shown.contains("tok-secret-4711"), "{shown}");
        assert!(shown.contains("db:write"), "{shown}");
    }
}
