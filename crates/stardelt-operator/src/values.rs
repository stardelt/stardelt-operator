//! Per-component Helm values: golden defaults, inline overrides, and the
//! deterministic merge order between them.
//!
//! This module is pure on purpose, so every merge rule is unit-testable without
//! a cluster.
//!
//! Merge order per component, later wins:
//!
//! 1. the operator's golden defaults ([`golden_values`]),
//! 2. `spec.values` (free-form YAML inline in the CR),
//! 3. `spec.set` (passed to Helm as `--set`, which Helm always applies last).

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
        // The data must outlive the pod: emptyDir/hostPath wipe on restart and
        // orphan the Iceberg metadata pointing at the files. The S3 endpoint is
        // what the catalog and engines talk to, so it is on by default.
        "seaweedfs" => json_obj(&[
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

/// The effective values for one component: golden defaults, then the CR's
/// inline `values` (see the module docs for the full order; `set` is handed to
/// Helm as `--set` and is not part of this document).
pub fn effective_values(component: &str, inline: Option<&Value>) -> Value {
    let mut values = golden_values(component);
    if let Some(inline) = inline {
        values = deep_merge(values, inline.clone());
    }
    values
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
    }

    #[test]
    fn empty_overrides_leave_the_golden_document_untouched() {
        assert_eq!(effective_values("trino", None), golden_values("trino"));
        assert_eq!(
            effective_values("trino", Some(&serde_json::json!({}))),
            golden_values("trino")
        );
    }

    #[test]
    fn inline_values_beat_golden_defaults() {
        let inline = serde_json::json!({
            "coordinator": { "jvm": { "maxHeapSize": "4G" } }
        });
        let merged = effective_values("trino", Some(&inline));
        assert_eq!(
            merged["coordinator"]["jvm"]["maxHeapSize"],
            serde_json::json!("4G")
        );
    }

    #[test]
    fn golden_defaults_survive_an_unrelated_override() {
        let inline = serde_json::json!({
            "coordinator": { "resources": { "limits": { "cpu": "2" } } }
        });
        let merged = effective_values("trino", Some(&inline));
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
}
