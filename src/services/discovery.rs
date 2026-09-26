//! Typed provider capabilities shared by native Host and the Server adapter.

use std::collections::HashSet;

use serde::Serialize;

use crate::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct ProviderDescriptor {
    pub id: String,
    pub name: String,
    pub capabilities: Vec<String>,
    pub authentication_required: bool,
    pub availability: String,
    pub generated: bool,
    pub curated: bool,
    pub result_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_template: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProviderRegistry {
    pub providers: Vec<ProviderDescriptor>,
    pub gallery_dl_version: Option<String>,
}

impl ProviderRegistry {
    pub fn descriptor(&self, id: &str) -> Option<&ProviderDescriptor> {
        let normalized = if id == "gallery-dl" { "local" } else { id };
        self.providers
            .iter()
            .find(|provider| provider.id == normalized)
    }

    pub fn ids(&self) -> HashSet<String> {
        self.providers
            .iter()
            .map(|provider| provider.id.clone())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderCatalog {
    pub providers: Vec<ProviderDescriptor>,
    pub gallery_dl_version: Option<String>,
}

pub fn providers(state: &AppState) -> Result<ProviderCatalog, &'static str> {
    if !state.edition.owns_library() {
        return Err("Viewer cannot inspect a local discovery registry");
    }
    if state.shutdown.is_cancelled() {
        return Err("Curator is shutting down");
    }
    Ok(ProviderCatalog {
        providers: state.search_registry.providers.clone(),
        gallery_dl_version: state.search_registry.gallery_dl_version.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    #[tokio::test]
    async fn direct_and_http_provider_catalogs_match() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let direct = providers(&state).unwrap();
        let http = crate::routes::search::providers(State(state)).await.0;
        assert_eq!(serde_json::to_value(direct).unwrap(), http);
    }

    #[test]
    fn viewer_local_registry_is_denied() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert!(providers(&viewer).is_err());
    }
}
