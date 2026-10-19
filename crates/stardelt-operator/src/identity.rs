//! The identity contract (decision D20): the ConfigMap `stardelt-identity` every building block
//! reads, built from `PlatformInstance.spec.identity` and `spec.baseDomain`. Pure functions, no I/O.
//!
//! The keys are the ones `stardelt-platform` writes today (`chart/templates/identity.yaml`), so a
//! component cannot tell whether the chart or the operator produced them.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::ObjectMeta;

use crate::crd::{IdentityMode, PlatformInstanceSpec};

/// Name of the ConfigMap in the platform namespace.
pub const CONFIGMAP_NAME: &str = "stardelt-identity";
/// Label on that ConfigMap carrying the mode, as the chart sets it.
pub const MODE_LABEL: &str = "stardelt.io/identity-mode";
/// Realm of the platform's own Keycloak.
pub const PLATFORM_REALM: &str = "stardelt";

/// Why the settings cannot be turned into a contract.
#[derive(Debug, PartialEq, Eq)]
pub struct IdentityError(pub String);

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for IdentityError {}

/// The issuer URL components must trust: derived for `platform`, given for `external`.
pub fn issuer_url(spec: &PlatformInstanceSpec) -> Result<Option<String>, IdentityError> {
    match spec.identity.mode {
        IdentityMode::None => Ok(None),
        IdentityMode::Platform => {
            let domain = base_domain(spec)?;
            Ok(Some(format!("https://id.{domain}/realms/{PLATFORM_REALM}")))
        }
        IdentityMode::External => {
            let url = spec.identity.issuer_url.as_deref().ok_or_else(|| {
                IdentityError("identity.mode external needs identity.issuerUrl".into())
            })?;
            if !url.starts_with("https://") {
                return Err(IdentityError(
                    "identity.issuerUrl must be an https:// URL".into(),
                ));
            }
            Ok(Some(url.trim_end_matches('/').to_string()))
        }
    }
}

/// The base domain of the instance, or the reason it cannot be used. `keycloak.rs` renders the
/// realm hosts and `KC_HOSTNAME` from the same value.
pub(crate) fn base_domain(spec: &PlatformInstanceSpec) -> Result<&str, IdentityError> {
    spec.base_domain
        .as_deref()
        .ok_or_else(|| IdentityError("identity needs baseDomain".into()))
}

/// The data of ConfigMap `stardelt-identity`, or `None` when sign-in is off (no ConfigMap exists
/// then; components read its absence as "no identity provider").
pub fn contract(
    spec: &PlatformInstanceSpec,
) -> Result<Option<BTreeMap<String, String>>, IdentityError> {
    let Some(issuer) = issuer_url(spec)? else {
        return Ok(None);
    };
    let domain = base_domain(spec)?;
    Ok(Some(BTreeMap::from([
        ("issuerUrl".to_string(), issuer),
        ("groupClaim".to_string(), spec.identity.group_claim.clone()),
        ("adminGroup".to_string(), spec.identity.admin_group.clone()),
        ("authMode".to_string(), "oidc".to_string()),
        // Nova answers on the base domain itself; every other application on a subdomain.
        ("publicUrl".to_string(), format!("https://{domain}")),
    ])))
}

/// The ConfigMap `stardelt-identity` to apply in `namespace`, or `None` when sign-in is off.
///
/// It carries no owner reference: uninstalling the platform keeps the contract (decision D18,
/// `deletionPolicy: Retain`), and the chart or `scripts/identity-bootstrap.sh` may already own
/// other keys of the same object (for example `trinoUrl`), which server-side apply leaves alone.
pub fn config_map(
    spec: &PlatformInstanceSpec,
    namespace: &str,
) -> Result<Option<ConfigMap>, IdentityError> {
    let Some(data) = contract(spec)? else {
        return Ok(None);
    };
    Ok(Some(ConfigMap {
        metadata: ObjectMeta {
            name: Some(CONFIGMAP_NAME.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    "app.kubernetes.io/part-of".to_string(),
                    "stardelt".to_string(),
                ),
                (
                    MODE_LABEL.to_string(),
                    mode_label(spec.identity.mode).to_string(),
                ),
            ])),
            ..ObjectMeta::default()
        },
        data: Some(data),
        ..ConfigMap::default()
    }))
}

/// Value of [`MODE_LABEL`].
pub fn mode_label(mode: IdentityMode) -> &'static str {
    match mode {
        IdentityMode::None => "none",
        IdentityMode::Platform => "platform",
        IdentityMode::External => "external",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::IdentitySpec;

    fn spec(identity: IdentitySpec, base_domain: Option<&str>) -> PlatformInstanceSpec {
        serde_json::from_value(serde_json::json!({
            "identity": identity,
            "baseDomain": base_domain,
        }))
        .expect("a PlatformInstanceSpec")
    }

    fn external(url: &str) -> IdentitySpec {
        IdentitySpec {
            mode: IdentityMode::External,
            issuer_url: Some(url.into()),
            ..IdentitySpec::default()
        }
    }

    #[test]
    fn without_sign_in_there_is_no_contract() {
        let s = spec(IdentitySpec::default(), None);
        assert_eq!(contract(&s), Ok(None));
        assert_eq!(issuer_url(&s), Ok(None));
    }

    #[test]
    fn the_platform_mode_derives_the_keycloak_issuer_from_the_base_domain() {
        let s = spec(
            IdentitySpec {
                mode: IdentityMode::Platform,
                ..IdentitySpec::default()
            },
            Some("alpha.dev.stardelt.io"),
        );
        let data = contract(&s).unwrap().unwrap();
        assert_eq!(
            data["issuerUrl"],
            "https://id.alpha.dev.stardelt.io/realms/stardelt"
        );
        assert_eq!(data["publicUrl"], "https://alpha.dev.stardelt.io");
        assert_eq!(data["authMode"], "oidc");
        assert_eq!(data["groupClaim"], "groups");
        assert_eq!(data["adminGroup"], "stardelt-admins");
    }

    #[test]
    fn the_external_mode_passes_the_issuer_through_without_a_trailing_slash() {
        let s = spec(
            external("https://login.microsoftonline.com/tenant-id/v2.0/"),
            Some("data.example.com"),
        );
        let data = contract(&s).unwrap().unwrap();
        assert_eq!(
            data["issuerUrl"],
            "https://login.microsoftonline.com/tenant-id/v2.0"
        );
        assert_eq!(data["publicUrl"], "https://data.example.com");
    }

    #[test]
    fn custom_claim_and_admin_group_are_carried() {
        let mut id = external("https://idp.example.com");
        id.group_claim = "roles".into();
        id.admin_group = "data-platform-admins".into();
        let data = contract(&spec(id, Some("data.example.com")))
            .unwrap()
            .unwrap();
        assert_eq!(data["groupClaim"], "roles");
        assert_eq!(data["adminGroup"], "data-platform-admins");
    }

    #[test]
    fn broken_settings_are_errors() {
        let no_url = spec(
            IdentitySpec {
                mode: IdentityMode::External,
                ..IdentitySpec::default()
            },
            Some("d.example.com"),
        );
        assert!(contract(&no_url).unwrap_err().0.contains("issuerUrl"));
        let http = spec(external("http://idp.example.com"), Some("d.example.com"));
        assert!(contract(&http).unwrap_err().0.contains("https"));
        let no_domain = spec(external("https://idp.example.com"), None);
        assert!(contract(&no_domain).unwrap_err().0.contains("baseDomain"));
    }

    #[test]
    fn the_config_map_is_named_labelled_and_carries_the_contract() {
        let s = spec(
            external("https://idp.example.com"),
            Some("data.example.com"),
        );
        let cm = config_map(&s, "stardelt").unwrap().unwrap();
        assert_eq!(cm.metadata.name.as_deref(), Some("stardelt-identity"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = cm.metadata.labels.unwrap();
        assert_eq!(labels["stardelt.io/identity-mode"], "external");
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        assert!(
            cm.metadata.owner_references.is_none(),
            "uninstalling keeps the contract"
        );
        assert_eq!(cm.data.unwrap()["issuerUrl"], "https://idp.example.com");
    }

    #[test]
    fn without_sign_in_there_is_no_config_map_and_bad_settings_are_errors() {
        assert!(
            config_map(&spec(IdentitySpec::default(), None), "stardelt")
                .unwrap()
                .is_none()
        );
        assert!(
            config_map(
                &spec(external("http://x"), Some("d.example.com")),
                "stardelt"
            )
            .is_err()
        );
    }

    #[test]
    fn the_mode_label_matches_the_chart() {
        assert_eq!(mode_label(IdentityMode::None), "none");
        assert_eq!(mode_label(IdentityMode::Platform), "platform");
        assert_eq!(mode_label(IdentityMode::External), "external");
    }

    #[test]
    fn the_schema_carries_the_cel_rules_and_the_fields_default_sensibly() {
        use kube::CustomResourceExt;
        let crd = serde_json::to_string(&crate::crd::PlatformInstance::crd()).unwrap();
        assert!(crd.contains("identity.mode external needs identity.issuerUrl"));
        assert!(crd.contains("baseDomain"));
        let s: PlatformInstanceSpec = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(s.identity, IdentitySpec::default());
        assert_eq!(s.base_domain, None);
    }
}
