//! The registration checklist for `identity.mode: external` (decision D31): what a customer
//! must create in their own provider, per client. Pure functions, no cluster access.
//!
//! The hosts come from [`crate::identity::hosts`] and the redirect paths are the platform-mode
//! realm's (`keycloak_realm.json`), asserted against that file by a test — so the checklist
//! cannot tell the customer to register something the platform's own realm does not use.

use crate::crd::{IdentityMode, IdentityProvider, PlatformInstanceSpec};
use crate::identity::{self, IdentityError};

/// One app registration the customer makes in the external provider.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ClientChecklist {
    /// Client name, as in `spec.identity.clients`.
    pub client: &'static str,
    /// `"confidential"`, `"public"` or `"service"`.
    pub kind: &'static str,
    /// Redirect URIs with this instance's real hosts; empty for a service client.
    pub redirect_uris: Vec<String>,
    /// What the tokens of this client must carry in `aud`. Empty for a service client, whose
    /// tokens target the provider's admin API rather than the platform.
    pub audience: String,
    /// Provider-specific lines: token version, API scope, app roles, permissions, group claim.
    pub notes: Vec<String>,
}

/// What to register for every client the instance lists, in the realm's client order.
///
/// Only clients named in `spec.identity.clients` appear: the resource is the declaration of
/// which components this instance runs, so the output is its complete registration list.
pub fn checklist(spec: &PlatformInstanceSpec) -> Result<Vec<ClientChecklist>, IdentityError> {
    if spec.identity.mode != IdentityMode::External {
        return Err(IdentityError("the checklist is for external mode".into()));
    }
    identity::validate(spec)?;
    let hosts = identity::hosts(identity::base_domain(spec)?);
    let provider = spec.identity.provider;
    let provider_name = provider_name(provider);
    let api_audience = spec
        .identity
        .api_audience
        .as_deref()
        .ok_or_else(|| IdentityError("external mode needs apiAudience".into()))?;

    let mut entries = Vec::with_capacity(identity::CLIENT_NAMES.len());
    for name in identity::CLIENT_NAMES {
        let Some(client) = spec.identity.clients.get(name) else {
            continue;
        };
        let kind = kind(name);
        entries.push(ClientChecklist {
            client: name,
            kind,
            redirect_uris: redirect_uris(name, &hosts, provider_name),
            audience: audience(name, &client.client_id, api_audience),
            notes: notes(spec, name, kind, api_audience),
        });
    }
    Ok(entries)
}

/// The provider name each component's sign-in library uses for the provider: Entra has a branch
/// that reads the roles and the user principal name from the ID token, every other OIDC provider
/// is the generic `idp`.
fn provider_name(provider: IdentityProvider) -> &'static str {
    match provider {
        IdentityProvider::Entra => "azure",
        IdentityProvider::Oidc => "idp",
    }
}

/// The registration kind: the browser-facing `lakekeeper` is public (PKCE, no secret), the group
/// sync is a service client, everything else is a confidential web app.
fn kind(client: &str) -> &'static str {
    match client {
        "lakekeeper" => "public",
        "trino-group-sync" => "service",
        _ => "confidential",
    }
}

/// The redirect URIs of one client, with this instance's hosts and the provider's sign-in name.
fn redirect_uris(client: &str, hosts: &identity::Hosts, provider: &str) -> Vec<String> {
    match client {
        "nova" => vec![format!("https://{}/api/auth/callback", hosts.nova)],
        "trino" => vec![
            format!("https://{}/oauth2/callback", hosts.trino),
            format!("https://{}/ui/login.html", hosts.trino),
        ],
        "lakekeeper" => vec![format!("https://{}/ui/*", hosts.lakekeeper)],
        "superset" => vec![format!(
            "https://{}/oauth-authorized/{provider}",
            hosts.superset
        )],
        "airflow" => vec![format!(
            "https://{}/auth/oauth-authorized/{provider}",
            hosts.airflow
        )],
        "jupyterhub" => vec![format!("https://{}/hub/oauth_callback", hosts.jupyter)],
        // `trino-group-sync` signs in with the client credentials flow: no redirect URI.
        _ => Vec::new(),
    }
}

/// The audience one client's tokens must carry: the platform API audience for the engine, the
/// catalog and the notebook hub; the client's own id for the components that validate their own
/// tokens.
fn audience(client: &str, client_id: &str, api_audience: &str) -> String {
    match client {
        "superset" | "airflow" => client_id.to_string(),
        "trino-group-sync" => String::new(),
        _ => api_audience.to_string(),
    }
}

/// The provider-specific lines for one client. Entra needs a v2 token API app, one exposed scope
/// and app roles; every confidential client also gets the standard sign-in permissions. Any other
/// OIDC provider only needs to emit group names in its tokens.
fn notes(spec: &PlatformInstanceSpec, client: &str, kind: &str, api_audience: &str) -> Vec<String> {
    match spec.identity.provider {
        IdentityProvider::Entra => {
            let mut notes = vec![
                "set accessTokenAcceptedVersion to 2 in the manifest".to_string(),
                format!("expose the scope api://{api_audience}/access"),
                format!(
                    "create the app roles {} and stardelt-users and assign them to groups",
                    spec.identity.admin_group
                ),
            ];
            if client == "lakekeeper" {
                notes.push("register it as a single-page application (PKCE), not a web app".into());
            }
            if kind == "confidential" {
                notes.push("add the delegated permissions openid, profile, email".into());
            }
            if client == "trino-group-sync" {
                notes.push(
                    "grant the application permission Group.Read.All with admin consent".into(),
                );
            }
            notes
        }
        IdentityProvider::Oidc => vec![format!(
            "emit the claim {} with group names in the ID token and the access token",
            spec.identity.effective_group_claim()
        )],
    }
}

/// Render the checklist as plain text, one block per client, in [`identity::CLIENT_NAMES`] order.
pub fn render(entries: &[ClientChecklist]) -> String {
    /// Indentation that puts continuation lines under the value column.
    const VALUE_COLUMN: &str = "                  ";
    let mut out = String::new();
    for (index, entry) in entries.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str(entry.client);
        out.push_str(" (");
        out.push_str(entry.kind);
        out.push_str(")\n");
        out.push_str("  redirect URIs:  ");
        out.push_str(&joined(&entry.redirect_uris, VALUE_COLUMN));
        out.push('\n');
        out.push_str("  audience:       ");
        out.push_str(if entry.audience.is_empty() {
            "(none)"
        } else {
            entry.audience.as_str()
        });
        out.push('\n');
        out.push_str("  notes:          ");
        out.push_str(&joined(&entry.notes, VALUE_COLUMN));
        out.push('\n');
    }
    out
}

/// Join `values` on lines aligned under `indent`, or `(none)` for an empty list.
fn joined(values: &[String], indent: &str) -> String {
    if values.is_empty() {
        return "(none)".to_string();
    }
    values.join(&format!("\n{indent}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::PlatformInstance;

    const EXAMPLE: &str = include_str!("../../../examples/platforminstance-external-entra.yaml");

    fn spec(provider: IdentityProvider, domain: &str, clients: &[&str]) -> PlatformInstanceSpec {
        let clients: serde_json::Map<String, serde_json::Value> = clients
            .iter()
            .map(|name| {
                let mut client = serde_json::json!({ "clientId": format!("{name}-client-id") });
                if *name != "lakekeeper" {
                    client["clientSecretRef"] =
                        serde_json::json!({ "name": format!("idp-{name}") });
                }
                ((*name).to_string(), client)
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "baseDomain": domain,
            "identity": {
                "mode": "external",
                "provider": provider,
                "issuerUrl": "https://idp.example.com",
                "apiAudience": "api://stardelt",
                "clients": clients,
            },
        }))
        .expect("a PlatformInstanceSpec")
    }

    fn entry<'a>(entries: &'a [ClientChecklist], client: &str) -> &'a ClientChecklist {
        entries
            .iter()
            .find(|entry| entry.client == client)
            .unwrap_or_else(|| panic!("no {client} block"))
    }

    #[test]
    fn entra_example_lists_every_registered_client() {
        let instance: PlatformInstance = serde_yaml::from_str(EXAMPLE).expect("the example parses");
        let entries = checklist(&instance.spec).expect("the example is external");
        let listed: Vec<&str> = entries.iter().map(|entry| entry.client).collect();
        assert_eq!(
            listed,
            [
                "nova",
                "trino",
                "lakekeeper",
                "superset",
                "airflow",
                "jupyterhub"
            ]
        );
        assert_eq!(
            entries.len(),
            6,
            "the example registers six sign-in clients"
        );
        for name in instance.spec.identity.clients.keys() {
            assert!(
                listed.contains(&name.as_str()),
                "{name} is registered but not in the checklist"
            );
        }
        // The components that validate their own tokens use their registered client id.
        assert_eq!(
            entry(&entries, "superset").audience,
            instance.spec.identity.clients["superset"].client_id
        );
        assert_eq!(
            entry(&entries, "airflow").audience,
            instance.spec.identity.clients["airflow"].client_id
        );
        // Every Entra block carries the API app's token version and the exposed scope.
        for entry in &entries {
            assert!(
                entry
                    .notes
                    .iter()
                    .any(|note| note.contains("accessTokenAcceptedVersion")),
                "{entry:?}"
            );
            assert!(
                entry
                    .notes
                    .iter()
                    .any(|note| note.contains("api://11111111-1111-4111-8111-111111111111/access"))
            );
        }
    }

    #[test]
    fn hosts_come_from_the_base_domain() {
        let domain = "lab.example.org";
        let hosts = identity::hosts(domain);
        let entries = checklist(&spec(
            IdentityProvider::Oidc,
            domain,
            &[
                "nova",
                "trino",
                "lakekeeper",
                "superset",
                "airflow",
                "jupyterhub",
            ],
        ))
        .expect("a valid external spec");
        assert_eq!(
            entry(&entries, "nova").redirect_uris,
            [format!("https://{}/api/auth/callback", hosts.nova)]
        );
        assert_eq!(
            entry(&entries, "trino").redirect_uris,
            [
                format!("https://{}/oauth2/callback", hosts.trino),
                format!("https://{}/ui/login.html", hosts.trino),
            ]
        );
        assert_eq!(
            entry(&entries, "lakekeeper").redirect_uris,
            [format!("https://{}/ui/*", hosts.lakekeeper)]
        );
        assert_eq!(
            entry(&entries, "superset").redirect_uris,
            [format!("https://{}/oauth-authorized/idp", hosts.superset)]
        );
        assert_eq!(
            entry(&entries, "airflow").redirect_uris,
            [format!(
                "https://{}/auth/oauth-authorized/idp",
                hosts.airflow
            )]
        );
        assert_eq!(
            entry(&entries, "jupyterhub").redirect_uris,
            [format!("https://{}/hub/oauth_callback", hosts.jupyter)]
        );
        // Every URI carries the instance's domain, not a previous one.
        for entry in &entries {
            for uri in &entry.redirect_uris {
                assert!(
                    uri.starts_with(&format!("https://{domain}"))
                        || uri.contains(&format!(".{domain}/")),
                    "{uri}"
                );
            }
        }
    }

    #[test]
    fn redirect_paths_match_the_platform_realm_template() {
        let domain = "data.example.com";
        let entries = checklist(&spec(
            IdentityProvider::Oidc,
            domain,
            &[
                "nova",
                "trino",
                "lakekeeper",
                "superset",
                "airflow",
                "jupyterhub",
            ],
        ))
        .expect("a valid external spec");
        let realm: serde_json::Value = serde_json::from_str(crate::keycloak::REALM_TEMPLATE)
            .expect("the realm template is JSON");
        let hosts = identity::hosts(domain);
        let clients = realm["clients"].as_array().expect("clients");
        for entry in &entries {
            let client = clients
                .iter()
                .find(|client| client["clientId"] == entry.client)
                .unwrap_or_else(|| panic!("the realm has no {} client", entry.client));
            let realm_uris: Vec<String> = client["redirectUris"]
                .as_array()
                .map(|uris| {
                    uris.iter()
                        .map(|uri| {
                            uri.as_str()
                                .expect("a redirect URI")
                                .replace("__BASE_DOMAIN__", &hosts.nova)
                                .replace("__NOVA_HOST__", &hosts.nova)
                                .replace("__TRINO_HOST__", &hosts.trino)
                                .replace("__LAKEKEEPER_HOST__", &hosts.lakekeeper)
                                .replace("__SUPERSET_HOST__", &hosts.superset)
                                .replace("__AIRFLOW_HOST__", &hosts.airflow)
                                .replace("__JUPYTER_HOST__", &hosts.jupyter)
                        })
                        .collect()
                })
                .unwrap_or_default();
            for uri in &entry.redirect_uris {
                // The realm uses Keycloak's own client name where a component asks for the
                // provider's sign-in name; that suffix is the only difference.
                let expected = uri.replace("oauth-authorized/idp", "oauth-authorized/<provider>");
                assert!(
                    realm_uris.iter().any(|realm_uri| {
                        realm_uri
                            .replace("oauth-authorized/keycloak", "oauth-authorized/<provider>")
                            == expected
                    }),
                    "the realm has no {uri} for {}",
                    entry.client
                );
            }
        }
    }

    #[test]
    fn provider_name_is_azure_for_entra_and_idp_otherwise() {
        let hosts = identity::hosts("data.example.com");
        for (provider, name) in [
            (IdentityProvider::Entra, "azure"),
            (IdentityProvider::Oidc, "idp"),
        ] {
            assert_eq!(provider_name(provider), name);
            let entries = checklist(&spec(
                provider,
                "data.example.com",
                &["nova", "superset", "airflow"],
            ))
            .expect("a valid external spec");
            assert_eq!(
                entry(&entries, "superset").redirect_uris,
                [format!(
                    "https://{}/oauth-authorized/{name}",
                    hosts.superset
                )]
            );
            assert_eq!(
                entry(&entries, "airflow").redirect_uris,
                [format!(
                    "https://{}/auth/oauth-authorized/{name}",
                    hosts.airflow
                )]
            );
        }
    }

    #[test]
    fn public_client_is_lakekeeper_only() {
        let hosts = identity::hosts("data.example.com");
        let entries = checklist(&spec(
            IdentityProvider::Oidc,
            "data.example.com",
            &[
                "nova",
                "trino",
                "lakekeeper",
                "superset",
                "airflow",
                "jupyterhub",
                "trino-group-sync",
            ],
        ))
        .expect("a valid external spec");
        for entry in &entries {
            match entry.client {
                "lakekeeper" => assert_eq!(entry.kind, "public"),
                "trino-group-sync" => assert_eq!(entry.kind, "service"),
                _ => assert_eq!(entry.kind, "confidential", "{}", entry.client),
            }
        }
        assert_eq!(
            entry(&entries, "lakekeeper").redirect_uris,
            [format!("https://{}/ui/*", hosts.lakekeeper)]
        );
        let service = entry(&entries, "trino-group-sync");
        assert!(
            service.redirect_uris.is_empty(),
            "a service client has no redirect URI"
        );
        assert!(
            service.audience.is_empty(),
            "its tokens target the provider's admin API, not the platform"
        );
    }

    #[test]
    fn platform_mode_is_refused() {
        let platform: PlatformInstanceSpec = serde_json::from_value(serde_json::json!({
            "baseDomain": "data.example.com",
            "identity": { "mode": "platform" },
        }))
        .expect("a PlatformInstanceSpec");
        assert_eq!(
            checklist(&platform).unwrap_err().0,
            "the checklist is for external mode"
        );
        let none: PlatformInstanceSpec =
            serde_json::from_value(serde_json::json!({})).expect("a PlatformInstanceSpec");
        assert_eq!(
            checklist(&none).unwrap_err().0,
            "the checklist is for external mode"
        );
    }

    #[test]
    fn unlisted_clients_are_left_out() {
        let entries = checklist(&spec(
            IdentityProvider::Entra,
            "data.example.com",
            &["nova", "superset"],
        ))
        .expect("a valid external spec");
        let listed: Vec<&str> = entries.iter().map(|entry| entry.client).collect();
        assert_eq!(listed, ["nova", "superset"]);
        for name in [
            "trino",
            "lakekeeper",
            "airflow",
            "jupyterhub",
            "trino-group-sync",
        ] {
            assert!(!listed.contains(&name), "{name} is not registered");
        }
    }

    #[test]
    fn json_output_round_trips() {
        let entries = checklist(&spec(
            IdentityProvider::Entra,
            "data.example.com",
            &[
                "nova",
                "trino",
                "lakekeeper",
                "superset",
                "airflow",
                "jupyterhub",
                "trino-group-sync",
            ],
        ))
        .expect("a valid external spec");
        let document = serde_json::to_string(&entries).expect("the checklist is JSON");
        let parsed: serde_json::Value = serde_json::from_str(&document).expect("the JSON parses");
        assert_eq!(
            parsed,
            serde_json::to_value(&entries).expect("the checklist")
        );
        assert_eq!(parsed[0]["client"], "nova");
        assert_eq!(parsed[0]["kind"], "confidential");
        assert_eq!(parsed[0]["audience"], "api://stardelt");
        assert!(!parsed[0]["redirect_uris"].as_array().unwrap().is_empty());
        assert_eq!(parsed[6]["client"], "trino-group-sync");
        assert_eq!(parsed[6]["redirect_uris"], serde_json::json!([]));
    }
}
