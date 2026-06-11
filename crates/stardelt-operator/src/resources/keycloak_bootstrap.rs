//! The `keycloak-bootstrap` Job.
//!
//! Idempotently configures the Keycloak realm: creates the realm, a GitHub
//! identity-provider broker (creds from `spec.sso.githubBrokerSecret`), and the
//! Nova OIDC client (secret persisted to `nova-oidc` Secret). Uses `kcadm.sh`
//! from the Keycloak image. Mirrors `bootstrap.rs`.

use base64::Engine as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::api::PlatformInstance;
use crate::api::platform_instance::KeycloakSsoSpec;
use crate::resources::keycloak;

pub const JOB_NAME: &str = "keycloak-bootstrap";
/// Secret the Job writes the Nova client secret into; Nova reads it.
pub const NOVA_OIDC_SECRET: &str = "nova-oidc";

const SCRIPT: &str = r#"set -e
KC=/opt/keycloak/bin/kcadm.sh
$KC config credentials --server "$KC_URL" --realm master --user "$KC_ADMIN" --password "$KC_ADMIN_PASSWORD"

echo "▶ ensure realm $REALM"
if ! $KC get "realms/$REALM" >/dev/null 2>&1; then
  $KC create realms -s realm="$REALM" -s enabled=true
  echo "✓ realm created"
else
  echo "✓ realm exists"
fi

echo "▶ ensure github identity provider"
if ! $KC get "identity-provider/instances/github" -r "$REALM" >/dev/null 2>&1; then
  $KC create identity-provider/instances -r "$REALM" \
    -s alias=github -s providerId=github -s enabled=true \
    -s "config.clientId=$GH_CLIENT_ID" -s "config.clientSecret=$GH_CLIENT_SECRET"
  echo "✓ github broker created"
else
  echo "✓ github broker exists"
fi

echo "▶ ensure nova client"
CID=$($KC get clients -r "$REALM" -q clientId="$NOVA_CLIENT_ID" --fields id --format csv --noquotes | tail -n1)
if [ -z "$CID" ]; then
  $KC create clients -r "$REALM" \
    -s clientId="$NOVA_CLIENT_ID" -s enabled=true -s protocol=openid-connect \
    -s publicClient=false -s standardFlowEnabled=true \
    -s secret="$NOVA_CLIENT_SECRET" \
    -s "redirectUris=[\"https://nova.$DOMAIN/auth/callback\"]" \
    -s "rootUrl=https://nova.$DOMAIN"
  echo "✓ nova client created"
else
  echo "✓ nova client exists"
fi
echo "DONE"
"#;

pub fn build(pi: &PlatformInstance, sso: &KeycloakSsoSpec, owner: OwnerReference) -> Job {
    let ns = &pi.spec.namespace;
    let kc_url = keycloak::service_url(ns);
    let body = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": JOB_NAME, "namespace": ns,
            "labels": super::labels(),
            "ownerReferences": [serde_json::to_value(&owner).unwrap()],
        },
        "spec": {
            "backoffLimit": 6,
            "template": {
                "metadata": { "labels": super::labels() },
                "spec": {
                    "restartPolicy": "OnFailure",
                    "containers": [{
                        "name": "kcadm",
                        "image": keycloak::KEYCLOAK_IMAGE,
                        "command": ["/bin/bash", "-c", SCRIPT],
                        "env": [
                            { "name": "KC_URL", "value": kc_url },
                            { "name": "REALM", "value": sso.realm },
                            { "name": "DOMAIN", "value": sso.domain },
                            { "name": "NOVA_CLIENT_ID", "value": sso.nova_client_id },
                            { "name": "KC_ADMIN", "value": "admin" },
                            { "name": "KC_ADMIN_PASSWORD", "valueFrom": { "secretKeyRef": {
                                "name": keycloak::ADMIN_SECRET, "key": "admin-password" }}},
                            { "name": "GH_CLIENT_ID", "valueFrom": { "secretKeyRef": {
                                "name": sso.github_broker_secret, "key": "client-id" }}},
                            { "name": "GH_CLIENT_SECRET", "valueFrom": { "secretKeyRef": {
                                "name": sso.github_broker_secret, "key": "client-secret" }}},
                            { "name": "NOVA_CLIENT_SECRET", "valueFrom": { "secretKeyRef": {
                                "name": NOVA_OIDC_SECRET, "key": "client-secret" }}},
                        ],
                    }],
                }
            }
        }
    });
    serde_json::from_value(body).expect("keycloak-bootstrap Job is valid")
}

/// Deterministic Nova OIDC client secret, derived from the CR uid so it is
/// stable across reconciles (no RNG in the reconcile path). Applied as a Secret
/// BEFORE the bootstrap Job (which reads it) and read by Nova at runtime.
pub fn nova_oidc_secret(
    namespace: &str,
    uid: &str,
    owner: &serde_json::Value,
) -> serde_json::Value {
    // FNV-1a-style stable derivation of a 32-byte secret from the uid.
    let mut acc: u64 = 0xcbf29ce484222325;
    let mut bytes = Vec::with_capacity(32);
    for chunk in 0..4u8 {
        for b in format!("{uid}:{chunk}").bytes() {
            acc ^= b as u64;
            acc = acc.wrapping_mul(0x100000001b3);
        }
        bytes.extend_from_slice(&acc.to_be_bytes());
    }
    let client_secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes);
    let data_value = base64::engine::general_purpose::STANDARD.encode(client_secret.as_bytes());
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": NOVA_OIDC_SECRET, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "type": "Opaque",
        "data": { "client-secret": data_value }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    fn instance() -> PlatformInstance {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": { "namespace": "stardelt",
                "sso": { "domain": "lab.stardelt.io", "githubBrokerSecret": "keycloak-github-broker" } }
        }))
        .unwrap()
    }

    #[test]
    fn bootstrap_job_builds_with_broker_env() {
        let pi = instance();
        let sso = pi.spec.sso.clone().unwrap();
        let job = build(&pi, &sso, OwnerReference::default());
        let c = &job.spec.unwrap().template.spec.unwrap().containers[0];
        let env_names: Vec<&str> = c
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert!(env_names.contains(&"GH_CLIENT_ID"));
        assert!(env_names.contains(&"NOVA_CLIENT_SECRET"));
        assert!(c.image.as_ref().unwrap().contains("keycloak"));
    }

    #[test]
    fn nova_oidc_secret_is_stable_for_same_uid() {
        let s1 = nova_oidc_secret("stardelt", "uid-abc", &serde_json::json!({}));
        let s2 = nova_oidc_secret("stardelt", "uid-abc", &serde_json::json!({}));
        assert_eq!(s1["data"], s2["data"], "same uid -> same secret");
        assert_eq!(s1["metadata"]["name"], "nova-oidc");
        assert!(s1["data"]["client-secret"].as_str().unwrap().len() > 10);
    }
}
