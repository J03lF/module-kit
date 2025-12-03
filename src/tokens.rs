use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleTokenExchangeResponse {
    pub token: String,
    pub scopes: Vec<String>,
    pub expires_in_seconds: u64,
}
