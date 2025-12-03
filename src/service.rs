use serde::{Deserialize, Serialize};

/// Payload that Fenrir modules can expose under `/.fenrir/services` so the runtime
/// can register their service descriptors dynamically.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ModuleReportedServices {
    pub module_id: String,
    #[serde(default)]
    pub services: Vec<ModuleServiceDescriptor>,
}

impl ModuleReportedServices {
    pub fn new(module_id: impl Into<String>) -> Self {
        Self {
            module_id: module_id.into(),
            services: Vec::new(),
        }
    }

    pub fn with_service(mut self, descriptor: ModuleServiceDescriptor) -> Self {
        self.services.push(descriptor);
        self
    }

    pub fn push(&mut self, descriptor: ModuleServiceDescriptor) {
        self.services.push(descriptor);
    }
}

/// Service descriptor representation that matches Fenrir's runtime schema.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ModuleServiceDescriptor {
    pub service_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub route_prefix: Option<String>,
    #[serde(default)]
    pub health_path: Option<String>,
    #[serde(default)]
    pub internal_only: Option<bool>,
    #[serde(default)]
    pub ingress_access: Option<String>,
    #[serde(default)]
    pub protocols: Vec<String>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub allowed_roles: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl ModuleServiceDescriptor {
    pub fn builder(service_id: impl Into<String>) -> ModuleServiceDescriptorBuilder {
        ModuleServiceDescriptorBuilder::new(service_id.into())
    }
}

pub struct ModuleServiceDescriptorBuilder {
    inner: ModuleServiceDescriptor,
}

impl ModuleServiceDescriptorBuilder {
    fn new(service_id: String) -> Self {
        Self {
            inner: ModuleServiceDescriptor {
                service_id,
                protocols: vec!["http".to_string()],
                ..ModuleServiceDescriptor::default()
            },
        }
    }

    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.inner.name = Some(value.into());
        self
    }

    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.inner.description = Some(value.into());
        self
    }

    pub fn kind(mut self, value: impl Into<String>) -> Self {
        self.inner.kind = Some(value.into());
        self
    }

    pub fn route_prefix(mut self, value: impl Into<String>) -> Self {
        self.inner.route_prefix = Some(value.into());
        self
    }

    pub fn health_path(mut self, value: impl Into<String>) -> Self {
        self.inner.health_path = Some(value.into());
        self
    }

    pub fn internal_only(mut self, value: bool) -> Self {
        self.inner.internal_only = Some(value);
        if !value {
            self.inner.ingress_access = Some("public".to_string());
        }
        self
    }

    pub fn public(self) -> Self {
        self.internal_only(false)
    }

    pub fn ingress_access(mut self, value: impl Into<String>) -> Self {
        self.inner.ingress_access = Some(value.into());
        self
    }

    pub fn add_scope(mut self, value: impl Into<String>) -> Self {
        self.inner.required_scopes.push(value.into());
        self
    }

    pub fn add_scopes<I>(mut self, scopes: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        for scope in scopes {
            self.inner.required_scopes.push(scope.into());
        }
        self
    }

    pub fn add_role(mut self, value: impl Into<String>) -> Self {
        self.inner.allowed_roles.push(value.into());
        self
    }

    pub fn add_protocol(mut self, value: impl Into<String>) -> Self {
        self.inner.protocols.push(value.into());
        self
    }

    pub fn add_tag(mut self, value: impl Into<String>) -> Self {
        self.inner.tags.push(value.into());
        self
    }

    pub fn build(mut self) -> ModuleServiceDescriptor {
        if self.inner.protocols.is_empty() {
            self.inner.protocols.push("http".to_string());
        }
        self.inner
    }
}
