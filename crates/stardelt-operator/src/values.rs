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

use crate::crd::Profile;

/// Golden Helm values for one component: the curated settings that cannot be
/// left to chart defaults without breaking the stack.
///
/// Mirrors the cluster-independent subset of `stardelt-platform/helm-values/`
/// (the platform repo stays the source of truth for the pins). The curated
/// values, with their rationale, live in `values/*.yaml`; components without
/// curated values get an empty document, and every key can be overridden from
/// the CR.
pub fn golden_values(component: &str) -> Value {
    match component {
        "trino" => serde_yaml::from_str(include_str!("../values/trino.yaml"))
            .expect("values/trino.yaml is not valid YAML"),
        "lakekeeper" => serde_yaml::from_str(include_str!("../values/lakekeeper.yaml"))
            .expect("values/lakekeeper.yaml is not valid YAML"),
        // Sign-in goes through the platform IdP, exactly like
        // stardelt-platform/helm-values/jupyterhub.yaml: the hub reads the identity
        // contract (ConfigMap stardelt-identity, Secret stardelt-oidc-clients) of
        // its own namespace. There is no shared-password login.
        //
        // The curated values live in `values/jupyterhub.yaml` (wave 4, #101); the
        // only code-built part is the OIDC snippet, which the file deliberately
        // omits and this arm injects.
        "jupyterhub" => {
            let mut values: Value = serde_yaml::from_str(include_str!("../values/jupyterhub.yaml"))
                .expect("values/jupyterhub.yaml is not valid YAML");
            values["hub"]["extraConfig"]["10-oidc"] =
                Value::String(include_str!("jupyterhub_oidc.py").into());
            values
        }
        "seaweedfs" => serde_yaml::from_str(include_str!("../values/seaweedfs.yaml"))
            .expect("values/seaweedfs.yaml is not valid YAML"),
        _ => Value::Object(Map::new()),
    }
}

/// Sizing for one profile, applied between the golden values and anything the CR asks for.
///
/// The platform already thinks this way: `make install-core SLIM=1` layers
/// `helm-values/slim/` over the base values, and `install` does not. Mirroring that keeps one
/// story — `demo` is the shape that fits a small node or a CI runner (Trino heaps that fit
/// next to everything else, PVCs measured in gigabytes), while `full` and `airgap` are the
/// base values. `airgap` matches `full` for now: the air-gap kit is P4 work layered on top,
/// not a different size.
///
/// Anything here can be overridden by the CR: this is a default, not a policy.
pub fn profile_values(component: &str, profile: Profile) -> Value {
    if !matches!(profile, Profile::Demo) {
        return Value::Object(Map::new());
    }
    match component {
        "trino" => serde_yaml::from_str(include_str!("../values/trino.demo.yaml"))
            .expect("values/trino.demo.yaml is not valid YAML"),
        "seaweedfs" => serde_yaml::from_str(include_str!("../values/seaweedfs.demo.yaml"))
            .expect("values/seaweedfs.demo.yaml is not valid YAML"),
        _ => Value::Object(Map::new()),
    }
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
    profile: Profile,
    external: Option<&Value>,
    inline: Option<&Value>,
) -> Value {
    let mut values = deep_merge(golden_values(component), profile_values(component, profile));
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
            serde_json::json!("generic-oauth")
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
    fn jupyterhub_golden_values_hold_no_credential() {
        let text = golden_values("jupyterhub").to_string();
        assert!(!text.contains("DummyAuthenticator"));
        assert!(!text.contains("\"password\""));
        assert!(text.contains("stardelt-oidc-clients"));
    }

    #[test]
    fn jupyterhub_identity_references_are_optional() {
        // A cluster without `make install-identity` must still start the hub:
        // the ConfigMap/Secret references are optional, never required objects.
        let hub = golden_values("jupyterhub")["hub"].clone();
        assert_eq!(
            hub["extraEnv"]["STARDELT_OIDC_ISSUER_URL"]["valueFrom"]["configMapKeyRef"]["optional"],
            serde_json::json!(true)
        );
        assert_eq!(
            hub["extraEnv"]["STARDELT_OIDC_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]["optional"],
            serde_json::json!(true)
        );
        assert_eq!(
            hub["extraEnv"]["STARDELT_OIDC_ISSUER_URL"]["valueFrom"]["configMapKeyRef"]["name"],
            serde_json::json!("stardelt-identity")
        );
        assert_eq!(
            hub["extraEnv"]["STARDELT_OIDC_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]["name"],
            serde_json::json!("stardelt-oidc-clients")
        );
    }

    #[test]
    fn jupyterhub_golden_values_come_from_the_yaml_file() {
        let file: Value = serde_yaml::from_str(include_str!("../values/jupyterhub.yaml"))
            .expect("values/jupyterhub.yaml is valid YAML");
        assert!(file.is_object(), "the values file is a mapping");
        assert!(
            file["hub"]["extraConfig"].get("10-oidc").is_none(),
            "the OIDC snippet is injected in code, not stored in the file"
        );

        let oidc = include_str!("jupyterhub_oidc.py");
        let golden = golden_values("jupyterhub");
        assert_eq!(
            golden["hub"]["extraConfig"]["10-oidc"],
            Value::String(oidc.into())
        );

        // The file is the whole of the curation: everything the operator applies
        // is the file plus the one injected snippet.
        let mut expected = file;
        expected["hub"]["extraConfig"]["10-oidc"] = Value::String(oidc.into());
        assert_eq!(golden, expected);
    }

    #[test]
    fn jupyterhub_golden_values_round_trip_through_yaml() {
        let values = golden_values("jupyterhub");
        let document = serde_yaml::to_string(&values).expect("golden values serialize as YAML");
        let parsed: Value = serde_yaml::from_str(&document).expect("serialized values parse back");
        assert_eq!(parsed, values);
    }

    #[test]
    fn every_curated_values_file_is_a_non_empty_mapping() {
        for (name, document) in [
            ("lakekeeper.yaml", include_str!("../values/lakekeeper.yaml")),
            ("seaweedfs.yaml", include_str!("../values/seaweedfs.yaml")),
            ("trino.yaml", include_str!("../values/trino.yaml")),
            (
                "seaweedfs.demo.yaml",
                include_str!("../values/seaweedfs.demo.yaml"),
            ),
            ("trino.demo.yaml", include_str!("../values/trino.demo.yaml")),
        ] {
            let value: Value = serde_yaml::from_str(document)
                .unwrap_or_else(|e| panic!("values/{name} is not valid YAML: {e}"));
            let mapping = value
                .as_object()
                .unwrap_or_else(|| panic!("values/{name} is not a mapping"));
            assert!(!mapping.is_empty(), "values/{name} is empty");
        }
    }

    #[test]
    fn trino_golden_values_carry_the_warehouse_catalog() {
        for profile in [Profile::Demo, Profile::Full, Profile::Airgap] {
            let trino = effective_values("trino", profile, None, None);
            let warehouse = trino["catalogs"]["warehouse"]
                .as_str()
                .expect("catalogs.warehouse is a properties document");
            assert!(
                warehouse.contains("iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog"),
                "profile {profile:?}: {warehouse}"
            );
            assert!(
                warehouse.contains("s3.endpoint=http://seaweedfs-s3:8333"),
                "profile {profile:?}: {warehouse}"
            );
            // File-system cache (stardelt-platform#134): the four catalog props
            // that point at the worker-local cache directory.
            for prop in [
                "fs.cache.enabled=true",
                "fs.cache.directories=/var/trino/cache",
                "fs.cache.max-sizes=10GB",
                "fs.cache.ttl=7d",
            ] {
                assert!(
                    warehouse.contains(prop),
                    "profile {profile:?}: missing {prop} in {warehouse}"
                );
            }
            // The props without the mount hard-fail the coordinator at startup
            // (CrashLoop: Cannot write to cache directory /var/trino/cache),
            // so the cache volume must ride along on BOTH coordinator and
            // workers — and survive every profile overlay.
            for role in ["coordinator", "worker"] {
                assert_eq!(
                    trino[role]["additionalVolumes"],
                    serde_json::json!([{ "name": "trino-fscache", "emptyDir": { "sizeLimit": "12Gi" } }, { "name": "trino-dumps", "emptyDir": { "sizeLimit": "2Gi" } }]),
                    "profile {profile:?}: {role} lost the cache volume"
                );
                assert_eq!(
                    trino[role]["additionalVolumeMounts"],
                    serde_json::json!([{ "name": "trino-fscache", "mountPath": "/var/trino/cache" }, { "name": "trino-dumps", "mountPath": "/dumps" }]),
                    "profile {profile:?}: {role} lost the cache mount"
                );
                assert!(
                    !trino[role]["additionalVolumes"][0]["emptyDir"]
                        .as_object()
                        .is_some_and(|dir| dir.contains_key("medium")),
                    "profile {profile:?}: {role} cache must not be memory-backed"
                );
            }
            assert_eq!(
                trino["env"][0]["valueFrom"]["secretKeyRef"]["name"],
                serde_json::json!("stardelt-s3-creds"),
                "profile {profile:?}: the S3 access key must come from the install Secret"
            );
            assert_eq!(
                trino["env"][1]["valueFrom"]["secretKeyRef"]["key"],
                serde_json::json!("secret-key"),
                "profile {profile:?}"
            );
        }
    }

    /// A Trino size such as `600MB`, `1500m` or `2G`, in mebibytes.
    fn megabytes(size: &str) -> u64 {
        let unit_at = size
            .find(|c: char| !c.is_ascii_digit())
            .expect("a size has a unit");
        let (digits, unit) = size.split_at(unit_at);
        let n: u64 = digits.parse().expect("a size starts with digits");
        match unit {
            "MB" | "m" | "Mi" => n,
            "GB" | "G" | "Gi" => n * 1024,
            other => panic!("unknown size unit {other:?} in {size:?}"),
        }
    }

    #[test]
    fn trino_memory_guards_hold_in_every_profile() {
        // platform#54 / operator#73: one oversized query must fail closed
        // (EXCEEDED_LOCAL_MEMORY_LIMIT) instead of OOM-killing the worker. So the per-node
        // limit plus the heap headroom leaves at least 400MB of the heap free, and the
        // kubelet limit sits above the heap.
        let expected = [
            (Profile::Demo, "1500m", "600MB", "450MB", "1800Mi", "2200Mi"),
            (Profile::Full, "2G", "800MB", "600MB", "2300Mi", "3000Mi"),
            (Profile::Airgap, "2G", "800MB", "600MB", "2300Mi", "3000Mi"),
        ];
        for (profile, heap, per_node, headroom, request, limit) in expected {
            let trino = effective_values("trino", profile, None, None);
            for role in ["coordinator", "worker"] {
                let checks = [
                    ("/jvm/maxHeapSize", heap),
                    ("/config/query/maxMemoryPerNode", per_node),
                    ("/config/memory/heapHeadroomPerNode", headroom),
                    ("/resources/requests/memory", request),
                    ("/resources/limits/memory", limit),
                ];
                for (pointer, want) in checks {
                    assert_eq!(
                        trino[role].pointer(pointer),
                        Some(&serde_json::json!(want)),
                        "{profile:?} {role} {pointer}"
                    );
                }
                // The heap dump goes to the bounded emptyDir, in every profile.
                assert!(
                    trino[role]["additionalJVMConfig"]
                        .to_string()
                        .contains("-XX:HeapDumpPath=/dumps"),
                    "{profile:?} {role}: lost the heap-dump path"
                );
                assert!(
                    megabytes(per_node) + megabytes(headroom) + 400 <= megabytes(heap),
                    "{profile:?} {role}: per-node + headroom leave no room in the heap"
                );
                assert!(
                    megabytes(limit) > megabytes(heap),
                    "{profile:?} {role}: the kubelet limit must exceed the heap"
                );
            }
            // The cluster-wide limit equals the per-node limit, as in the platform values.
            assert_eq!(
                trino["server"]["config"]["query"]["maxMemory"],
                serde_json::json!(per_node),
                "{profile:?}"
            );
            // The time limits are coordinator-only: a string in
            // `server.coordinatorExtraConfig`, never in `additionalConfigProperties`
            // (the chart renders that one into the worker ConfigMap too).
            let extra = trino["server"]["coordinatorExtraConfig"]
                .as_str()
                .expect("a properties string");
            assert!(
                extra.contains("query.max-execution-time=10m"),
                "{profile:?}"
            );
            assert!(extra.contains("query.max-run-time=15m"), "{profile:?}");
            assert_eq!(
                trino["additionalConfigProperties"],
                serde_json::json!(["http-server.process-forwarded=true"]),
                "{profile:?}: coordinator-only keys must not reach the worker"
            );
        }
    }

    #[test]
    fn demo_profile_sizes_the_stack_for_a_small_node() {
        // Trino: the heap and the per-node memory limit are a pair — Trino refuses to start
        // if the limit does not fit the heap, which is why both are in the profile.
        let trino = effective_values("trino", Profile::Demo, None, None);
        assert_eq!(
            trino["coordinator"]["jvm"]["maxHeapSize"],
            serde_json::json!("1500m")
        );
        assert_eq!(
            trino["coordinator"]["config"]["query"]["maxMemoryPerNode"],
            serde_json::json!("600MB")
        );
        assert_eq!(trino["server"]["workers"], serde_json::json!(1));
        // SeaweedFS: gigabyte-scale volumes instead of the chart's defaults.
        let seaweedfs = effective_values("seaweedfs", Profile::Demo, None, None);
        assert_eq!(
            seaweedfs["volume"]["dataDirs"][0]["size"],
            serde_json::json!("5Gi")
        );
        // The profile must restate the whole entry: arrays replace, not merge.
        let dir = &seaweedfs["volume"]["dataDirs"][0];
        assert_eq!(dir["type"], serde_json::json!("persistentVolumeClaim"));
        assert_eq!(dir["name"], serde_json::json!("data"));
        assert_eq!(dir["maxVolumes"], serde_json::json!(100));
        // The golden values must survive the overlay: the profile sizes, it does not replace.
        assert_eq!(seaweedfs["s3"]["enabled"], serde_json::json!(true));
    }

    #[test]
    fn every_profile_keeps_seaweedfs_on_a_pvc() {
        for profile in [Profile::Demo, Profile::Full, Profile::Airgap] {
            let values = effective_values("seaweedfs", profile, None, None);
            assert_eq!(
                values["volume"]["dataDirs"][0]["type"],
                serde_json::json!("persistentVolumeClaim"),
                "profile {profile:?} dropped the PVC"
            );
        }
    }

    #[test]
    fn full_profile_leaves_sizing_to_the_charts() {
        for component in ["trino", "seaweedfs"] {
            assert_eq!(
                effective_values(component, Profile::Full, None, None),
                golden_values(component),
                "{component}: full adds no sizing of its own"
            );
        }
    }

    #[test]
    fn a_cr_value_beats_the_profile() {
        let inline = serde_json::json!({ "coordinator": { "jvm": { "maxHeapSize": "8G" } } });
        let merged = effective_values("trino", Profile::Demo, None, Some(&inline));
        assert_eq!(
            merged["coordinator"]["jvm"]["maxHeapSize"],
            serde_json::json!("8G")
        );
        // …and the rest of the profile still applies.
        assert_eq!(merged["server"]["workers"], serde_json::json!(1));
    }

    #[test]
    fn empty_overrides_leave_the_golden_document_untouched() {
        assert_eq!(
            effective_values("trino", Profile::Full, None, None),
            golden_values("trino")
        );
        assert_eq!(
            effective_values(
                "trino",
                Profile::Full,
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
        let merged = effective_values("trino", Profile::Full, Some(&external), Some(&inline));

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
        let merged = effective_values("trino", Profile::Full, None, Some(&inline));
        assert_eq!(
            merged["additionalConfigProperties"],
            serde_json::json!(["http-server.process-forwarded=true"]),
            "pinning resources must not drop the curated trino settings"
        );
        assert!(
            merged["catalogs"]["warehouse"]
                .as_str()
                .is_some_and(|catalog| catalog
                    .contains("iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog")),
            "pinning resources must not drop the warehouse catalog"
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

    #[test]
    fn lakekeeper_golden_values_use_the_operator_owned_database() {
        let l = golden_values("lakekeeper");
        assert_eq!(l["postgresql"]["enabled"], serde_json::json!(false));
        assert_eq!(
            l["externalDatabase"]["host_write"],
            serde_json::json!("lakekeeper-pg-rw")
        );
        assert_eq!(
            l["externalDatabase"]["passwordSecret"],
            serde_json::json!("lakekeeper-pg-app")
        );
        assert_eq!(l["authz"]["backend"], serde_json::json!("allowall"));
    }

    #[test]
    fn seaweedfs_golden_values_read_identities_from_the_generated_secret() {
        let s = golden_values("seaweedfs");
        assert_eq!(
            s["s3"]["existingConfigSecret"],
            serde_json::json!("seaweedfs-s3-config")
        );
        assert_eq!(s["s3"]["enableAuth"], serde_json::json!(true));
        assert_eq!(
            s["s3"]["createBuckets"][0]["name"],
            serde_json::json!("lakehouse")
        );
    }
}
