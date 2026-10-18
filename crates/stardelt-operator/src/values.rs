//! Per-component Helm values: golden defaults, external documents, inline
//! overrides, and the deterministic merge order between them.
//!
//! This module is pure on purpose — fetching a ConfigMap/Secret happens in the
//! controller, which hands the raw document to [`parse_values_document`]. That
//! keeps every merge rule unit-testable without a cluster.
//!
//! Merge order per component, later wins:
//!
//! 1. the operator's golden defaults ([`golden_values`]),
//! 2. `spec.valuesFrom` (external YAML document),
//! 3. `spec.values` (free-form YAML inline in the CR),
//! 4. `spec.set` (passed to Helm as `--set`, which Helm always applies last).

use serde_json::{Map, Value};

/// Golden Helm values for one component: the curated settings that cannot be
/// left to chart defaults without breaking the stack.
///
/// Mirrors the cluster-independent subset of `stardelt-platform/helm-values/`
/// (the platform repo stays the source of truth for the pins); components
/// without curated values yet get an empty document, and every key here can be
/// overridden from the CR.
pub fn golden_values(component: &str) -> Value {
    match component {
        // Trino sits behind ingress-nginx, which sets X-Forwarded-For; without
        // this it answers 406 "Server configuration does not allow processing
        // of the X-Forwarded-For header".
        "trino" => json_obj(&[(
            "additionalConfigProperties",
            serde_json::json!(["http-server.process-forwarded=true"]),
        )]),
        // Mirrors the cluster-independent subset of
        // stardelt-platform/helm-values/jupyterhub.yaml: the dev shape that fits a
        // notebook stack on one small node. `singleuser.storage: none` keeps the
        // per-user volume out of the way on a throwaway cluster, and the dummy
        // authenticator is the chart's, with the same documented throwaway
        // password the platform uses — an operator replacing it with the
        // platform's OIDC provider (the realm already defines the `jupyterhub`
        // client) does that through `spec.values`.
        "jupyterhub" => json_obj(&[
            (
                "hub",
                serde_json::json!({
                    "config": {
                        "JupyterHub": { "authenticator_class": "dummy" },
                        "DummyAuthenticator": { "password": "stardelt" }
                    },
                    "resources": {
                        "requests": { "cpu": "50m", "memory": "256Mi" },
                        "limits": { "cpu": "500m", "memory": "512Mi" }
                    }
                }),
            ),
            (
                "proxy",
                serde_json::json!({
                    "service": { "type": "ClusterIP" },
                    "chp": {
                        "resources": {
                            "requests": { "cpu": "50m", "memory": "128Mi" },
                            "limits": { "cpu": "500m", "memory": "256Mi" }
                        }
                    },
                    "traefik": {
                        "resources": {
                            "requests": { "cpu": "50m", "memory": "128Mi" },
                            "limits": { "cpu": "500m", "memory": "256Mi" }
                        }
                    }
                }),
            ),
            (
                "singleuser",
                serde_json::json!({
                    "image": { "name": "quay.io/jupyter/scipy-notebook", "tag": "2024-10-07" },
                    "defaultUrl": "/lab",
                    "startTimeout": 300,
                    "memory": { "limit": "1G", "guarantee": "256M" },
                    "cpu": { "limit": 1, "guarantee": 0.1 },
                    "storage": { "type": "none" }
                }),
            ),
        ]),
        // The data must outlive the pod: emptyDir/hostPath wipe on restart and
        // orphan the Iceberg metadata pointing at the files. The S3 endpoint is
        // what the catalog and engines talk to, so it is on by default.
        "seaweedfs" => json_obj(&[
            // The chart's swap/migration hook ships a cluster-scoped ClusterRole
            // (`seaweedfs-rw-cr`). Cluster-scoped means one per cluster, so leaving
            // it on makes the second lakehouse in a cluster fail to install — and
            // several lakehouses per cluster is the point of this CRD. Off here for
            // the same reason the platform chart sets it off.
            (
                "global",
                serde_json::json!({ "seaweedfs": { "createClusterRole": false } }),
            ),
            (
                "master",
                serde_json::json!({
                    "data": { "type": "persistentVolumeClaim" }
                }),
            ),
            (
                "volume",
                serde_json::json!({
                    "dataDirs": [{
                        "name": "data",
                        "type": "persistentVolumeClaim",
                        "maxVolumes": 0
                    }]
                }),
            ),
            (
                "filer",
                serde_json::json!({
                    "data": { "type": "persistentVolumeClaim" }
                }),
            ),
            ("s3", serde_json::json!({ "enabled": true })),
        ]),
        _ => Value::Object(Map::new()),
    }
}

/// Build a values object from `(key, value)` pairs.
fn json_obj(pairs: &[(&str, Value)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect(),
    )
}

/// Deep-merge `over` onto `base`.
///
/// Objects merge recursively; every other value — scalars, arrays, null —
/// replaces what was there. Arrays replacing is Helm's own value-file merge
/// behaviour, so what an admin writes here behaves exactly like a `-f` file.
pub fn deep_merge(base: Value, over: Value) -> Value {
    match (base, over) {
        (Value::Object(mut base), Value::Object(over)) => {
            for (key, value) in over {
                let merged = match base.remove(&key) {
                    Some(existing) => deep_merge(existing, value),
                    None => value,
                };
                base.insert(key, merged);
            }
            Value::Object(base)
        }
        // Not both objects: the override replaces the default outright.
        (_, over) => over,
    }
}

/// Parse a free-form values document (`valuesFrom`).
///
/// A Helm values document is a mapping; anchors/aliases and multi-document
/// files are not accepted (serde_yaml rejects the latter).
pub fn parse_values_document(document: &str) -> Result<Value, ValuesError> {
    let value: Value =
        serde_yaml::from_str(document).map_err(|e| ValuesError::InvalidYaml(e.to_string()))?;
    match value {
        Value::Object(_) => Ok(value),
        other => Err(ValuesError::NotAMapping {
            found: type_name(&other),
        }),
    }
}

/// The effective values for one component: golden defaults, then the external
/// document, then the CR's inline values (see the module docs for the full
/// order; `set` is handed to Helm as `--set` and is not part of this document).
pub fn effective_values(
    component: &str,
    external: Option<&Value>,
    inline: Option<&Value>,
) -> Value {
    let mut values = golden_values(component);
    for over in [external, inline].into_iter().flatten() {
        values = deep_merge(values, over.clone());
    }
    values
}

/// A `values` problem worth reporting in the CR status.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValuesError {
    #[error("not valid YAML: {0}")]
    InvalidYaml(String),
    #[error("a values document must be a YAML mapping, got {found}")]
    NotAMapping { found: &'static str },
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "a mapping",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_components_get_an_empty_document() {
        assert_eq!(golden_values("polaris"), serde_json::json!({}));
        assert_eq!(golden_values("cnpg"), serde_json::json!({}));
    }

    #[test]
    fn golden_values_carry_the_curated_settings() {
        let trino = golden_values("trino");
        assert_eq!(
            trino["additionalConfigProperties"],
            serde_json::json!(["http-server.process-forwarded=true"])
        );

        let seaweedfs = golden_values("seaweedfs");
        assert_eq!(seaweedfs["s3"]["enabled"], serde_json::json!(true));
        assert_eq!(
            seaweedfs["volume"]["dataDirs"][0]["type"],
            serde_json::json!("persistentVolumeClaim")
        );
        // A cluster-scoped role would make the second Lakehouse in a cluster
        // uninstallable, so the golden values must keep it off.
        assert_eq!(
            seaweedfs["global"]["seaweedfs"]["createClusterRole"],
            serde_json::json!(false)
        );
    }

    #[test]
    fn jupyterhub_golden_values_mirror_the_platform_dev_shape() {
        let j = golden_values("jupyterhub");
        assert_eq!(
            j["hub"]["config"]["JupyterHub"]["authenticator_class"],
            serde_json::json!("dummy")
        );
        assert_eq!(
            j["proxy"]["service"]["type"],
            serde_json::json!("ClusterIP")
        );
        assert_eq!(
            j["singleuser"]["storage"]["type"],
            serde_json::json!("none")
        );
        assert_eq!(j["singleuser"]["defaultUrl"], serde_json::json!("/lab"));
    }

    #[test]
    fn empty_overrides_leave_the_golden_document_untouched() {
        assert_eq!(
            effective_values("trino", None, None),
            golden_values("trino")
        );
        assert_eq!(
            effective_values(
                "trino",
                Some(&serde_json::json!({})),
                Some(&serde_json::json!({}))
            ),
            golden_values("trino")
        );
    }

    #[test]
    fn inline_values_beat_golden_and_external_documents() {
        let external = serde_json::json!({
            "additionalConfigProperties": ["external.setting=true"],
            "coordinator": { "jvm": { "maxHeapSize": "3G", "gcMethod": { "type": "UseZGC" } } }
        });
        let inline = serde_json::json!({
            "coordinator": { "jvm": { "maxHeapSize": "4G" } }
        });
        let merged = effective_values("trino", Some(&external), Some(&inline));

        // Inline beats the external document, which beats the golden default.
        assert_eq!(
            merged["coordinator"]["jvm"]["maxHeapSize"],
            serde_json::json!("4G")
        );
        assert_eq!(
            merged["additionalConfigProperties"],
            serde_json::json!(["external.setting=true"])
        );
        // Untouched keys from deeper layers survive the deep merge.
        assert_eq!(
            merged["coordinator"]["jvm"]["gcMethod"]["type"],
            serde_json::json!("UseZGC")
        );
    }

    #[test]
    fn golden_defaults_survive_an_unrelated_override() {
        let inline = serde_json::json!({
            "coordinator": { "resources": { "limits": { "cpu": "2" } } }
        });
        let merged = effective_values("trino", None, Some(&inline));
        assert_eq!(
            merged["additionalConfigProperties"],
            serde_json::json!(["http-server.process-forwarded=true"]),
            "pinning resources must not drop the curated trino settings"
        );
    }

    #[test]
    fn arrays_replace_instead_of_merging_like_helm() {
        let base = serde_json::json!({ "list": ["a", "b"] });
        let over = serde_json::json!({ "list": ["c"] });
        assert_eq!(deep_merge(base, over), serde_json::json!({ "list": ["c"] }));
    }

    #[test]
    fn parsing_accepts_a_yaml_mapping() {
        let parsed = parse_values_document(
            "volume:\n  dataDirs:\n    - name: data\n      storageClass: fast-ssd\n",
        )
        .expect("valid mapping");
        assert_eq!(
            parsed["volume"]["dataDirs"][0]["storageClass"],
            serde_json::json!("fast-ssd")
        );
    }

    #[test]
    fn parsing_rejects_non_mappings() {
        assert_eq!(
            parse_values_document("- a\n- b\n"),
            Err(ValuesError::NotAMapping { found: "a list" })
        );
        assert_eq!(
            parse_values_document("42\n"),
            Err(ValuesError::NotAMapping { found: "a number" })
        );
        assert_eq!(
            parse_values_document(""),
            Err(ValuesError::NotAMapping { found: "null" })
        );
    }

    #[test]
    fn parsing_rejects_malformed_yaml() {
        let err = parse_values_document("a: [unclosed").expect_err("must not parse");
        assert!(
            matches!(err, ValuesError::InvalidYaml(_)),
            "unexpected error: {err}"
        );
    }
}
