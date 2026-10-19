//! The identity contract (decision D20): the ConfigMap `stardelt-identity` every building block
//! reads, built from `PlatformInstance.spec.identity` and `spec.baseDomain`. Pure functions, no I/O.
//!
//! The keys are the ones `stardelt-platform` writes today (`chart/templates/identity.yaml`), so a
//! component cannot tell whether the chart or the operator produced them.

use std::collections::BTreeMap;

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

fn base_domain(spec: &PlatformInstanceSpec) -> Result<&str, IdentityError> {
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
