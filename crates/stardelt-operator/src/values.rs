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
        // The `pipelines` pillar: the platform's helm-values/airflow.yaml with short in-cluster
        // addresses and no Reloader annotation (see the header of the file).
        "airflow" => serde_yaml::from_str(include_str!("../values/airflow.yaml"))
            .expect("values/airflow.yaml is not valid YAML"),
        // The `dashboards` pillar: the platform's helm-values/superset.yaml with short in-cluster
        // addresses. The analyst-role script is injected from src/ because the chart mounts
        // `extraConfigs` as a ConfigMap and the init Job runs what is there, exactly like the
        // platform's `--set-file` in its Makefile. The OIDC/login snippet is injected here
        // because it is a Python file, like `jupyterhub_oidc.py`.
        "superset" => {
            let mut values: Value = serde_yaml::from_str(include_str!("../values/superset.yaml"))
                .expect("values/superset.yaml is not valid YAML");
            values["configOverrides"]["oauth"] =
                Value::String(include_str!("superset_oidc.py").into());
            values["extraConfigs"]["superset-analyst-role.py"] =
                Value::String(include_str!("superset_analyst_role.py").into());
            values
        }
        // The catalog's permission store (O5): the platform's helm-values/openfga.yaml, verbatim —
        // it has no namespace-qualified address (the DSN comes from Secret `openfga-pg-app`).
        "openfga" => serde_yaml::from_str(include_str!("../values/openfga.yaml"))
            .expect("values/openfga.yaml is not valid YAML"),
        // The secrets contract (decision D23): External Secrets Operator, verbatim from
        // stardelt-platform/helm-values/external-secrets.yaml — its three workloads are small and the
        // resource requests are the platform's.
        "external-secrets" => serde_yaml::from_str(include_str!("../values/external-secrets.yaml"))
            .expect("values/external-secrets.yaml is not valid YAML"),
        // The sandbox secret store (decision D23, platform mode): the platform's
        // helm-values/openbao.yaml verbatim — standalone on one volume, auto-unseal by the static key
        // in Secret openbao-seal-key, audit to stdout.
        "openbao" => serde_yaml::from_str(include_str!("../values/openbao.yaml"))
            .expect("values/openbao.yaml is not valid YAML"),
        "kafka" => serde_yaml::from_str(include_str!("../values/kafka.yaml"))
            .expect("values/kafka.yaml is not valid YAML"),
        "spark-operator" => serde_yaml::from_str(include_str!("../values/spark-operator.yaml"))
            .expect("values/spark-operator.yaml is not valid YAML"),
        _ => Value::Object(Map::new()),
    }
}

/// Values layered over a component when the platform has an identity provider. The
/// controller decides that from the cluster (ConfigMap `stardelt-identity` and Trino's key
/// material); the file itself lives next to the golden values so the platform's
/// `helm-values/trino-identity.yaml` and this one can be diffed by eye.
pub fn identity_values(component: &str) -> Value {
    match component {
        "trino" => serde_yaml::from_str(include_str!("../values/trino-identity.yaml"))
            .expect("values/trino-identity.yaml is not valid YAML"),
        // Superset's datasources move to the public Trino host, which exists only with an IdP.
        "superset" => serde_yaml::from_str(include_str!("../values/superset-identity.yaml"))
            .expect("values/superset-identity.yaml is not valid YAML"),
        _ => Value::Object(Map::new()),
    }
}

/// Values that exist only with the streaming pillar (D42): the demo Airflow DAGs publish to the
/// bus. Empty `kafka.bootstrap` in the golden file renders no `PEGELONLINE_KAFKA_BOOTSTRAP`, so a
/// platform without the pillar never points a task at a Service that does not exist.
pub fn streaming_values(component: &str) -> Value {
    match component {
        "airflow" => serde_json::json!({"kafka": {"bootstrap": "kafka-kafka-bootstrap:9092"}}),
        _ => Value::Object(Map::new()),
    }
}

/// Replace every `__BASE_DOMAIN__` in the strings of a values document. The identity overlays
/// are checked-in files and cannot read `spec.baseDomain`; `identity_checklist.rs` uses the
/// same placeholder for its commands.
pub fn substitute_base_domain(values: &mut Value, domain: &str) {
    match values {
        Value::String(text) => *text = text.replace("__BASE_DOMAIN__", domain),
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| substitute_base_domain(item, domain)),
        Value::Object(map) => map
            .values_mut()
            .for_each(|item| substitute_base_domain(item, domain)),
        _ => {}
    }
}

/// Replace every `__NAMESPACE__` in the strings of a values document. The values files are static
/// but `spec.namespace` is not (operator#37), so an in-cluster address that must follow the
/// namespace carries the placeholder and is filled in when the plan is applied.
pub fn substitute_namespace(values: &mut Value, namespace: &str) {
    match values {
        Value::String(text) => *text = text.replace("__NAMESPACE__", namespace),
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| substitute_namespace(item, namespace)),
        Value::Object(map) => map
            .values_mut()
            .for_each(|item| substitute_namespace(item, namespace)),
        _ => {}
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
            serde_json::json!([
                "http-server.process-forwarded=true",
                "cache-manager.config-files=etc/cache-manager-alluxio.properties",
            ])
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
    fn the_spark_operator_webhook_is_on() {
        // The mutating webhook copies a SparkApplication's volumes and volume mounts into the
        // driver/executor pods; without it a StreamApp's job file is never mounted (operator#314).
        let s = golden_values("spark-operator");
        assert_eq!(s["webhook"]["enable"], serde_json::json!(true));
    }

    #[test]
    fn the_spark_operator_keeps_the_submission_webhook_and_memory_budget() {
        let v = crate::values::golden_values("spark-operator");
        assert_eq!(v["webhook"]["enable"], true);
        assert_eq!(v["controller"]["resources"]["limits"]["memory"], "2Gi");
        assert_eq!(v["controller"]["resources"]["requests"]["memory"], "256Mi");
    }

    #[test]
    fn the_strimzi_operator_matches_the_chart_and_the_platform_path() {
        // stardelt-platform install-kafka installs this chart with no values, so the chart's
        // defaults are the platform path; the operator path must not lower them (operator#216).
        let v = crate::values::golden_values("kafka");
        assert_eq!(v["resources"]["requests"]["cpu"], "200m");
        assert_eq!(v["resources"]["requests"]["memory"], "384Mi");
        assert_eq!(v["resources"]["limits"]["cpu"], "1");
        assert_eq!(v["resources"]["limits"]["memory"], "384Mi");
    }

    #[test]
    fn openfga_golden_values_mirror_the_platform_shape() {
        let o = golden_values("openfga");
        assert_eq!(
            o["datastore"]["uriSecret"],
            serde_json::json!("openfga-pg-app")
        );
        assert_eq!(
            o["datastore"]["migrationType"],
            serde_json::json!("initContainer")
        );
        assert_eq!(
            o["image"]["repository"],
            serde_json::json!("openfga/openfga")
        );
        assert_eq!(o["image"]["tag"], serde_json::json!("v1.22.1"));
        assert_eq!(
            o["livenessProbe"]["enabled"],
            serde_json::json!(false),
            "a liveness probe turns a node stall into a restart, and a restart into a catalog outage"
        );
    }

    #[test]
    fn the_openfga_values_keep_the_platform_shape() {
        // stardelt-platform/helm-values/openfga.yaml carries the same block (#516). This test
        // cannot read the other repository; it only states the shape the two must share.
        let o = golden_values("openfga");
        assert_eq!(o["livenessProbe"]["enabled"], serde_json::json!(false));
        assert_eq!(
            o["resources"]["limits"]["memory"],
            serde_json::json!("512Mi"),
            "192 Mi was OOMKilled on the dev cluster (stardelt-platform#544); the platform carries the same"
        );
        assert_eq!(
            o["resources"]["requests"]["memory"],
            serde_json::json!("128Mi")
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
            serde_json::json!("dynamic")
        );
        assert_eq!(
            j["singleuser"]["storage"]["capacity"],
            serde_json::json!("2Gi"),
            "platform#696: a stopped notebook server keeps the person's files"
        );
        // An empty storage class is a real class name to the chart: it writes
        // `storageClassName: ""` on claim-<user>, which means "bind only to a
        // pre-made volume", not "use the cluster default" (platform#786,
        // operator#445). The key must be absent or name a class.
        let class = &j["singleuser"]["storage"]["dynamic"]["storageClass"];
        assert!(
            class.is_null() || class.as_str().is_some_and(|s| !s.is_empty()),
            "singleuser.storage.dynamic.storageClass must be absent or a real class, got {class}"
        );
        assert_eq!(j["singleuser"]["defaultUrl"], serde_json::json!("/lab"));
        // The Nova workspace service (operator#484, the operator half of platform#832):
        // the hub exposes a service, the role carries the four scopes, and the idle
        // culler the Nova countdown mirrors.
        assert_eq!(
            j["hub"]["services"]["nova"],
            serde_json::json!({}),
            "the hub needs a `nova` service so the chart generates its token"
        );
        assert_eq!(
            j["hub"]["loadRoles"]["nova-workspace"]["scopes"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            j["hub"]["networkPolicy"]["ingress"][0]["ports"][0]["port"],
            8081
        );
        assert_eq!(j["cull"]["timeout"].as_i64(), Some(3600));
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
    fn trino_posts_the_query_log_to_nova() {
        let values = golden_values("trino");
        assert_eq!(
            values["eventListenerProperties"][4],
            serde_json::json!(
                "http-event-listener.connect-http-headers=Authorization:Bearer ${ENV:NOVA_EVENTS_TOKEN},X-Stardelt-Warehouse:default"
            )
        );
        let env = values["env"].as_array().expect("env");
        let token_env = env
            .iter()
            .find(|e| e["name"] == "NOVA_EVENTS_TOKEN")
            .expect("NOVA_EVENTS_TOKEN env var");
        assert_eq!(
            token_env["valueFrom"]["secretKeyRef"],
            serde_json::json!({ "name": "nova-events", "key": "token", "optional": true })
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

        // The snippet is discovery-driven (#611): the provider's document names the
        // three endpoints, and a provider outage falls back to the realm layout
        // instead of stopping the hub.
        assert!(
            oidc.contains(".well-known/openid-configuration"),
            "the snippet must fetch the discovery document"
        );
        assert!(
            oidc.contains("authorization_endpoint"),
            "the snippet must read authorization_endpoint from the document"
        );
        assert!(
            oidc.contains("STARDELT_HUB_OAUTH_CALLBACK_URL"),
            "the snippet must still derive the public callback"
        );

        // The file is the whole of the curation: everything the operator applies
        // is the file plus the one injected snippet.
        let mut expected = file;
        expected["hub"]["extraConfig"]["10-oidc"] = Value::String(oidc.into());
        assert_eq!(golden, expected);
    }

    #[test]
    fn notebook_task_image_matches_the_single_user_image() {
        let airflow: Value = serde_yaml::from_str(include_str!("../values/airflow.yaml"))
            .expect("values/airflow.yaml is valid YAML");
        // PIPELINE_NOTEBOOK_IMAGE lives in the `extraEnv: |` block scalar, which is a
        // Helm-templated string (the `{{- if }}` lines make it invalid YAML), so read it
        // as text: the first `value:` line after the name marker.
        let extra_env = airflow["extraEnv"]
            .as_str()
            .expect("values/airflow.yaml has an extraEnv block scalar");
        let task = extra_env
            .split("- name: PIPELINE_NOTEBOOK_IMAGE")
            .nth(1)
            .expect("PIPELINE_NOTEBOOK_IMAGE in extraEnv")
            .lines()
            .map(str::trim)
            .find_map(|line| line.strip_prefix("value:"))
            .expect("PIPELINE_NOTEBOOK_IMAGE has a value")
            .trim();
        let hub: Value = serde_yaml::from_str(include_str!("../values/jupyterhub.yaml"))
            .expect("values/jupyterhub.yaml is valid YAML");
        let hub_tag = hub["singleuser"]["image"]["tag"]
            .as_str()
            .expect("singleuser.image.tag is a string");
        assert_eq!(
            task.split('@').nth(1),
            hub_tag.split('@').nth(1),
            "PIPELINE_NOTEBOOK_IMAGE in values/airflow.yaml must equal \
             singleuser.image.tag in values/jupyterhub.yaml"
        );
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
            ("jupyterhub.yaml", include_str!("../values/jupyterhub.yaml")),
            ("airflow.yaml", include_str!("../values/airflow.yaml")),
            ("superset.yaml", include_str!("../values/superset.yaml")),
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
            // File-system cache (stardelt-platform#134): Trino 483+ keeps only
            // the switch in the catalog. The directories, size and TTL are the
            // node-level cache manager's, below.
            for prop in ["fs.cache.enabled=true", "fs.s3.enabled=true"] {
                assert!(
                    warehouse.contains(prop),
                    "profile {profile:?}: missing {prop} in {warehouse}"
                );
            }
            for gone in [
                "fs.cache.directories",
                "fs.cache.max-sizes",
                "fs.cache.ttl",
                "fs.native-s3.enabled",
            ] {
                assert!(
                    !warehouse.contains(gone),
                    "profile {profile:?}: the catalog still sets {gone}; Trino 484 refuses it"
                );
            }
            // Every node loads the cache manager through config.properties and
            // the file itself must exist on both roles.
            assert!(
                trino["additionalConfigProperties"]
                    .as_array()
                    .is_some_and(|props| props.contains(&serde_json::json!(
                        "cache-manager.config-files=etc/cache-manager-alluxio.properties"
                    ))),
                "profile {profile:?}: config.properties does not name the cache manager"
            );
            for role in ["coordinator", "worker"] {
                let file = trino[role]["additionalConfigFiles"]["cache-manager-alluxio.properties"]
                    .as_str()
                    .unwrap_or_else(|| {
                        panic!("profile {profile:?}: {role} lacks the cache-manager file")
                    });
                for line in [
                    "cache-manager.name=alluxio",
                    "fs.cache.directories=/var/trino/cache",
                    "fs.cache.max-sizes=10GB",
                    "fs.cache.ttl=7d",
                ] {
                    assert!(
                        file.contains(line),
                        "profile {profile:?}: {role} cache-manager file lacks {line}"
                    );
                }
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

    #[test]
    fn the_trino_catalog_signs_in_as_the_catalog_principal() {
        let merged = deep_merge(golden_values("trino"), identity_values("trino"));
        let warehouse = merged["catalogs"]["warehouse"]
            .as_str()
            .expect("catalogs.warehouse is a properties document");
        for needle in [
            "iceberg.rest-catalog.security=OAUTH2",
            "iceberg.rest-catalog.oauth2.server-uri=${ENV:CATALOG_TOKEN_URL}",
            "iceberg.rest-catalog.oauth2.credential=${ENV:CATALOG_CLIENT_ID}:${ENV:CATALOG_CLIENT_SECRET}",
            "iceberg.rest-catalog.oauth2.scope=openid",
            "fs.cache.enabled=true",
            "fs.s3.enabled=true",
            "s3.endpoint=http://seaweedfs-s3:8333",
        ] {
            assert!(
                warehouse.contains(needle),
                "the identity overlay lacks `{needle}`"
            );
        }
        assert!(
            !warehouse.contains("fs.native-s3.enabled")
                && !warehouse.contains("fs.cache.directories"),
            "the identity overlay still carries 480-era cache properties"
        );
        let env = merged["env"].as_array().expect("env");
        let id = env
            .iter()
            .find(|e| e["name"] == "CATALOG_CLIENT_ID")
            .expect("CATALOG_CLIENT_ID");
        assert_eq!(id["valueFrom"]["secretKeyRef"]["name"], "sp-catalog-trino");
        assert!(
            env.iter().any(|e| e["name"] == "CATALOG_TOKEN_URL"
                && e["value"] == "$(TRINO_OIDC_ISSUER)/protocol/openid-connect/token"),
            "CATALOG_TOKEN_URL must derive from the identity contract"
        );
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
                serde_json::json!([
                    "http-server.process-forwarded=true",
                    "cache-manager.config-files=etc/cache-manager-alluxio.properties",
                ]),
                "{profile:?}: coordinator-only keys must not reach the worker, and the \
                 node-level cache manager must stay"
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
            for pointer in [
                "/volume/dataDirs/0/type",
                "/master/data/type",
                "/filer/data/type",
            ] {
                assert_eq!(
                    values.pointer(pointer),
                    Some(&serde_json::json!("persistentVolumeClaim")),
                    "profile {profile:?} dropped the PVC at {pointer}"
                );
            }
            // The chart has no default size for the PVC type, so an empty request
            // renders as `storage:` with no value and the API server refuses it.
            for pointer in [
                "/master/data/size",
                "/volume/dataDirs/0/size",
                "/filer/data/size",
            ] {
                let size = values
                    .pointer(pointer)
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                assert!(
                    !size.is_empty(),
                    "profile {profile:?}: {pointer} is empty, the PVC would be invalid"
                );
            }
        }
    }

    #[test]
    fn full_profile_leaves_the_trino_sizing_to_the_chart() {
        // Trino only: the SeaweedFS chart does not size a PVC, so the golden
        // values must carry one (see `every_profile_keeps_seaweedfs_on_a_pvc`).
        assert_eq!(
            effective_values("trino", Profile::Full, None, None),
            golden_values("trino"),
            "trino: full adds no sizing of its own"
        );
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
            serde_json::json!([
                "http-server.process-forwarded=true",
                "cache-manager.config-files=etc/cache-manager-alluxio.properties",
            ]),
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

    #[test]
    fn the_golden_airflow_values_run_tasks_as_pods_and_name_the_operators_secrets() {
        let values = golden_values("airflow");
        assert_eq!(values["executor"], "KubernetesExecutor");
        // The Secret names the chart reads are the ones the operator creates (airflow::SECRETS).
        let names: Vec<&str> = crate::airflow::SECRETS.iter().map(|s| s.name).collect();
        for field in [
            "fernetKeySecretName",
            "apiSecretKeySecretName",
            "webserverSecretKeySecretName",
            "jwtSecretName",
        ] {
            let name = values[field].as_str().expect(field);
            assert!(
                names.contains(&name),
                "{field} names {name}, which the operator does not create"
            );
        }
        assert_eq!(values["fernetKeySecretName"], "airflow-fernet-key");
        // #611: the pipeline runner's token endpoint comes from the identity contract.
        let extra_env = values["extraEnv"].as_str().expect("extraEnv");
        assert!(
            extra_env.contains("key: tokenEndpoint"),
            "PIPELINE_TOKEN_URL must come from the identity contract"
        );
        assert!(
            !extra_env.contains("http://keycloak:8080"),
            "no literal Keycloak token URL may remain in the Airflow values"
        );
    }

    /// platform#680: Nova's Airflow service user `nova-api` holds `stardelt-nova-operator` —
    /// Viewer plus run, pause and stop — because Nova's Run/Pause/Resume call Airflow as it.
    #[test]
    fn the_golden_airflow_create_user_job_gives_nova_api_the_operator_role() {
        let values = golden_values("airflow");
        let script = values["createUserJob"]["args"][2]
            .as_str()
            .expect("the script");
        assert!(
            script.contains("sm.add_role(\"stardelt-nova-operator\")"),
            "the nova-api service user needs the stardelt-nova-operator role"
        );
        assert!(
            script.contains("sm.create_permission(action, resource)"),
            "the operator role is Viewer plus the write permissions"
        );
        assert!(
            script.contains("sm.update_user(nova)"),
            "the created user must be moved onto the operator role"
        );
        assert!(
            script.contains("airflow users reset-password"),
            "re-running the Job must set the Secret's password on the existing user"
        );
        assert!(
            script.find("import os") < script.find("os.environ["),
            "os is imported before the environment is read"
        );
        assert_eq!(
            script.matches("--role Viewer").count(),
            1,
            "nova-api is created as Viewer and then moved; no second write role"
        );
        assert!(
            !script.contains("nova-service"),
            "the golden values must not create a second write role for Nova"
        );
    }

    #[test]
    fn the_golden_airflow_webserver_config_maps_group_roles() {
        let text = include_str!("../values/airflow.yaml");
        for needle in [
            "_AuthRolesMapping",
            "get_all_roles",
            "stardelt-group-",
            "AUTH_ROLES_SYNC_AT_LOGIN = True",
            "[(_admin_group, [\"Admin\"])]",
        ] {
            assert!(
                text.contains(needle),
                "the webserver config must contain {needle}"
            );
        }
    }

    #[test]
    fn superset_golden_values_pin_the_image_and_carry_sign_in() {
        let values = golden_values("superset");
        assert_eq!(
            values["image"]["repository"],
            serde_json::json!("ghcr.io/stardelt/superset")
        );
        assert_eq!(
            values["image"]["tag"],
            serde_json::json!(
                "dev@sha256:cbbc7dd6f0e676b0d01c86e4102ec0d4d711041a9ca120af393e81a4c761cb13"
            )
        );
        let config_overrides = values["configOverrides"].as_object().unwrap();
        assert!(config_overrides.contains_key("iframe_embedding"));
        assert!(!config_overrides.contains_key("password_store"));
        let oauth = values["configOverrides"]["oauth"].as_str().unwrap();
        assert_eq!(oauth, include_str!("superset_oidc.py"));
        assert!(oauth.contains("AUTH_TYPE = AUTH_OAUTH"));
        assert!(oauth.contains("_AuthRolesMapping"));
        assert!(oauth.contains("CUSTOM_SECURITY_MANAGER"));
        assert!(oauth.contains("STARDELT_SUPERSET_CLIENT_SECRET"));
        assert!(oauth.contains("CACHE_IMPERSONATION"));
        assert!(oauth.contains("SQLALCHEMY_CUSTOM_PASSWORD_STORE = _stardelt_password_store"));
        let text = include_str!("../values/superset.yaml");
        assert!(!text.contains("password_store"));
        assert!(!text.contains("AUTH_TYPE"));
        let extra_env = values["extraEnvRaw"].as_array().unwrap();
        let issuer = extra_env
            .iter()
            .find(|e| e["name"] == "STARDELT_OIDC_ISSUER")
            .unwrap();
        assert_eq!(
            issuer["valueFrom"]["configMapKeyRef"]["name"],
            "stardelt-identity"
        );
        assert_eq!(issuer["valueFrom"]["configMapKeyRef"]["key"], "issuerUrl");
        let secret = extra_env
            .iter()
            .find(|e| e["name"] == "STARDELT_SUPERSET_CLIENT_SECRET")
            .unwrap();
        assert_eq!(
            secret["valueFrom"]["secretKeyRef"]["name"],
            "stardelt-oidc-clients"
        );
        assert_eq!(secret["valueFrom"]["secretKeyRef"]["key"], "superset");
        assert!(text.contains("superset-analyst-role.py"));
        assert!(!text.contains("superset-group-roles.py"));
        let init_script = values["init"]["initscript"].as_str().unwrap();
        assert!(!init_script.contains("superset-group-roles.py"));
        let import = values["extraConfigs"]["import_datasources.yaml"]
            .as_str()
            .unwrap();
        assert!(import.contains("trino://superset@trino:8080/"));
        assert!(import.contains("warehouse"));
        assert!(import.contains("shared"));
        assert!(
            !serde_json::to_string(&golden_values("superset"))
                .unwrap()
                .contains("__BASE_DOMAIN__")
        );
    }

    #[test]
    fn superset_init_converges_the_analyst_role() {
        let values = golden_values("superset");
        assert_eq!(
            values["extraConfigs"]["superset-analyst-role.py"],
            include_str!("superset_analyst_role.py")
        );
        let script = include_str!("superset_analyst_role.py");
        assert!(script.contains("catalog_from_uri"));
        assert!(script.contains("\"shared\""));
        assert!(script.contains("stardelt-analyst"));
        let init_script = values["init"]["initscript"].as_str().unwrap();
        assert!(
            init_script
                .contains("python \"{{ .Values.extraConfigMountPath }}/superset-analyst-role.py\"")
        );
        assert!(init_script.contains("Converging the analyst role"));
        let text = include_str!("../values/superset.yaml");
        assert!(!text.contains("catalog_from_uri"));
        let import = values["extraConfigs"]["import_datasources.yaml"]
            .as_str()
            .unwrap();
        assert!(import.contains("trino://superset@trino:8080/shared"));
    }

    #[test]
    fn the_golden_airflow_values_pin_one_image_in_both_places_and_name_no_namespace() {
        let text = include_str!("../values/airflow.yaml");
        let values = golden_values("airflow");
        let digest = values["images"]["airflow"]["digest"]
            .as_str()
            .expect("a digest");
        assert_eq!(
            digest, "sha256:465ca8862f9a886a42891c57aa9156194e153b082aace744fb2a5eb29ad7e211",
            "the golden Airflow image must be the Airflow 3.2.2 build (demos#77) with the loader's access_control (platform#493) and the PEGELONLINE demo DAGs (demos#46); and a notebook task's CONN_ variables (demos#64); and the refusal of a connection without a synced Secret (demos#65)"
        );
        let worker = values["env"]
            .as_array()
            .expect("env")
            .iter()
            .find(|e| e["name"] == "AIRFLOW__KUBERNETES_EXECUTOR__WORKER_CONTAINER_TAG")
            .expect("the worker tag");
        assert!(
            worker["value"].as_str().expect("a tag").ends_with(digest),
            "scheduler and worker pods must run the same digest"
        );
        assert!(
            !text.contains(".svc.cluster.local"),
            "the namespace is not written into the values"
        );
        assert!(
            !text.contains("reloader.stakater.com"),
            "the operator installs no Reloader, so no pod asks it to restart"
        );
        assert!(text.contains("http://seaweedfs-s3:8333"));
        assert!(text.contains("http://lakekeeper:8181/catalog"));
    }

    #[test]
    fn the_golden_user_job_creates_the_administrator_and_the_service_user_of_nova() {
        let values = golden_values("airflow");
        let job = &values["createUserJob"];
        let env: Vec<(&str, &str, &str)> = job["env"]
            .as_array()
            .expect("env")
            .iter()
            .map(|e| {
                let source = &e["valueFrom"]["secretKeyRef"];
                (
                    e["name"].as_str().expect("a name"),
                    source["name"].as_str().expect("a Secret"),
                    source["key"].as_str().expect("a key"),
                )
            })
            .collect();
        assert_eq!(
            env,
            [
                ("AIRFLOW_ADMIN_USERNAME", "airflow-admin", "username"),
                ("AIRFLOW_ADMIN_PASSWORD", "airflow-admin", "password"),
                ("AIRFLOW_NOVA_API_USERNAME", "airflow-nova-api", "username"),
                ("AIRFLOW_NOVA_API_PASSWORD", "airflow-nova-api", "password"),
            ]
        );
        let script = job["args"][2].as_str().expect("the script");
        assert_eq!(script.matches("airflow users create").count(), 2);
        assert!(script.contains("airflow users reset-password"));
        assert!(script.contains("--role Admin") && script.contains("--role Viewer"));
        assert!(script.contains("--username \"$AIRFLOW_NOVA_API_USERNAME\""));
        assert!(script.starts_with("set -e"));
        assert!(
            !script.contains("exec "),
            "the second command must still run"
        );
    }

    #[test]
    fn people_who_sign_in_are_members_that_see_no_dag_until_a_group_role_grants_it() {
        let values = golden_values("airflow");
        let config = values["apiServer"]["apiServerConfig"]
            .as_str()
            .expect("the web server config");
        assert!(config.contains("AUTH_USER_REGISTRATION_ROLE = \"stardelt-member\""));
        assert!(!config.contains("AUTH_USER_REGISTRATION_ROLE = \"Viewer\""));
        let script = values["createUserJob"]["args"][2]
            .as_str()
            .expect("the script");
        assert!(script.contains("sm.add_role(\"stardelt-member\")"));
        assert!(script.contains("sm.find_role(\"Viewer\").permissions"));
        assert!(
            script.contains("GLOBAL = {(\"can_read\", \"DAGs\"), (\"can_read\", \"DAG Runs\")}")
        );
    }

    #[test]
    fn trino_identity_values_turn_authentication_on() {
        let values = identity_values("trino");
        assert_eq!(
            values["server"]["config"]["authenticationType"],
            serde_json::json!("PASSWORD,OAUTH2")
        );
        assert_eq!(
            values["server"]["config"]["https"]["port"],
            serde_json::json!(8443)
        );
        assert_eq!(
            values["auth"]["passwordAuthSecret"],
            serde_json::json!("trino-password-db")
        );
        assert_eq!(
            values["coordinator"]["secretMounts"][0]["secretName"],
            serde_json::json!("trino-coordinator-tls")
        );
        let extra = values["server"]["coordinatorExtraConfig"]
            .as_str()
            .expect("a string");
        assert!(extra.contains("http-server.authentication.allow-insecure-over-http=false"));
        assert!(
            extra.contains("http-server.authentication.oauth2.issuer=${ENV:TRINO_OIDC_ISSUER}")
        );
        assert!(
            extra.contains("query.max-execution-time=10m"),
            "the base guards are restated"
        );
        let props = values["additionalConfigProperties"]
            .as_array()
            .expect("a list");
        assert!(props.contains(&serde_json::json!(
            "internal-communication.shared-secret=${ENV:TRINO_INTERNAL_SHARED_SECRET}"
        )));
        assert!(props.contains(&serde_json::json!("http-server.process-forwarded=true")));

        let merged = deep_merge(
            effective_values(
                "trino",
                crate::crd::Profile::Full,
                None,
                Some(&serde_json::json!({
                    "server": {"config": {"authenticationType": "NONE"}}
                })),
            ),
            identity_values("trino"),
        );
        assert_eq!(
            merged["server"]["config"]["authenticationType"],
            serde_json::json!("PASSWORD,OAUTH2")
        );
    }

    /// platform#565: the coordinator must not exit 100 when the IdP is briefly
    /// unavailable. The gate is a chart-level `initContainers.coordinator` entry
    /// that polls the same discovery URL the coordinator uses.
    #[test]
    fn trino_identity_values_wait_for_the_idp() {
        let values = identity_values("trino");
        let init = &values["initContainers"]["coordinator"][0];
        assert_eq!(init["name"], serde_json::json!("wait-for-idp"));
        assert_eq!(init["image"], serde_json::json!("trinodb/trino:484"));
        assert_eq!(
            init["env"][0]["valueFrom"]["configMapKeyRef"]["name"],
            serde_json::json!("stardelt-identity")
        );
        assert_eq!(
            init["env"][0]["valueFrom"]["configMapKeyRef"]["key"],
            serde_json::json!("issuerUrl")
        );
        assert_eq!(
            init["env"][0]["valueFrom"]["configMapKeyRef"]["optional"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn only_trino_has_identity_values() {
        for component in ["lakekeeper", "airflow", "cnpg"] {
            assert_eq!(
                identity_values(component),
                serde_json::json!({}),
                "{component}"
            );
        }
    }

    #[test]
    fn streaming_values_set_the_airflow_bus_address() {
        assert_eq!(
            streaming_values("airflow")["kafka"]["bootstrap"],
            serde_json::json!("kafka-kafka-bootstrap:9092")
        );
        assert_eq!(streaming_values("lakekeeper"), serde_json::json!({}));
    }

    #[test]
    fn trino_identity_values_configure_the_group_and_warehouse_files() {
        let values = identity_values("trino");
        let coordinator = &values["coordinator"];
        assert_eq!(
            coordinator["additionalConfigFiles"]["resource-groups.properties"]
                .as_str()
                .unwrap(),
            "resource-groups.configuration-manager=file\n\
             resource-groups.config-file=/etc/trino/warehouses/resource-groups.json\n"
        );
        assert!(
            coordinator["additionalConfigFiles"]["group-provider.properties"]
                .as_str()
                .unwrap()
                .contains("file.group-file=/etc/trino/groups/groups.txt")
        );
        let volumes: Vec<_> = coordinator["additionalVolumes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            volumes,
            vec![
                "trino-groups",
                "trino-warehouses",
                "trino-fscache",
                "trino-dumps"
            ]
        );
        let mounts: Vec<_> = coordinator["additionalVolumeMounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["mountPath"].as_str().unwrap())
            .collect();
        assert_eq!(
            mounts,
            vec![
                "/etc/trino/groups",
                "/etc/trino/warehouses",
                "/var/trino/cache",
                "/dumps"
            ]
        );
    }

    #[test]
    fn the_golden_trino_values_stay_anonymous() {
        // The overlay is the only place with an authenticator: a cluster without an IdP must
        // not render one, and a CR must not be able to make it silent either (the overlay is
        // merged last by the controller).
        let text = include_str!("../values/trino.yaml");
        assert!(!text.contains("authenticationType"));
        assert!(!text.contains("passwordAuthSecret"));
        assert!(!text.contains("allow-insecure-over-http=false"));
    }

    #[test]
    fn trino_identity_values_carry_the_file_access_control_rules() {
        let values = identity_values("trino");
        assert_eq!(values["accessControl"]["type"], "configmap");
        let rules: serde_json::Value = serde_json::from_str(
            values["accessControl"]["rules"]["rules.json"]
                .as_str()
                .expect("rules.json"),
        )
        .expect("rules.json is a JSON document");
        let catalogs = rules["catalogs"].as_array().expect("catalogs");
        assert!(
            catalogs
                .iter()
                .any(|r| r["group"] == "stardelt-admins" && r["allow"] == "all")
        );
        assert!(
            catalogs
                .iter()
                .any(|r| r["catalog"] == "(system|tpch|tpcds)" && r["allow"] == "none")
        );
        assert!(
            rules["impersonation"]
                .as_array()
                .expect("impersonation")
                .iter()
                .any(|r| {
                    r["original_user"] == "superset"
                        && r["allow"] == serde_json::json!(true)
                        && r["new_user"].as_str().unwrap().contains("[0-9a-f]{8}-")
                })
        );
        // The OPA bridge file does not exist in this crate: listing it would stop Trino.
        let extra = values["server"]["coordinatorExtraConfig"].as_str().unwrap();
        assert!(!extra.contains("access-control.config-files"));
        // The no-IdP path stays without access control (the overlay is merged last).
        assert!(effective_values("trino", Profile::Full, None, None)["accessControl"].is_null());
    }

    #[test]
    fn superset_identity_values_name_the_public_trino_host() {
        let values = identity_values("superset");
        let import = values["extraConfigs"]["import_datasources.yaml"]
            .as_str()
            .expect("import_datasources.yaml");
        assert!(import.contains("trino://superset@trino.__BASE_DOMAIN__:443/warehouse"));
        assert!(import.contains("trino://superset@trino.__BASE_DOMAIN__:443/shared"));
        assert!(import.contains("impersonate_user: true"));
    }

    #[test]
    fn base_domain_is_substituted_in_strings_only() {
        let mut values =
            serde_json::json!({"a": "x.__BASE_DOMAIN__", "b": ["y.__BASE_DOMAIN__"], "n": 1});
        substitute_base_domain(&mut values, "example.com");
        assert_eq!(values["a"], serde_json::json!("x.example.com"));
        assert_eq!(values["b"][0], serde_json::json!("y.example.com"));
        assert_eq!(values["n"], serde_json::json!(1));
    }

    #[test]
    fn namespace_placeholder_is_substituted_in_strings_only() {
        let mut v = serde_json::json!({"a": "x.__NAMESPACE__", "b": ["y.__NAMESPACE__"], "n": 1});
        substitute_namespace(&mut v, "lab");
        assert_eq!(v["a"], serde_json::json!("x.lab"));
        assert_eq!(v["b"][0], serde_json::json!("y.lab"));
        assert_eq!(v["n"], serde_json::json!(1));
    }

    #[test]
    fn the_notebook_reaches_spark_and_the_store_in_its_own_namespace() {
        let mut values = effective_values("jupyterhub", Profile::Full, None, None);
        substitute_namespace(&mut values, "data");
        let single = &values["singleuser"];
        assert_eq!(
            single["extraEnv"]["SPARK_REMOTE"],
            serde_json::json!("sc://spark-connect.data.svc.cluster.local:15002")
        );
        assert_eq!(
            single["extraEnv"]["STARDELT_SECRET_STORE_URL"],
            serde_json::json!("http://openbao.data.svc.cluster.local:8200")
        );
        let rules = single["networkPolicy"]["egress"]
            .as_array()
            .expect("egress rules");
        assert_eq!(rules.len(), 3);
        assert_eq!(
            rules[1]["to"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/name"],
            serde_json::json!("spark-connect")
        );
        assert_eq!(rules[1]["ports"][0]["port"], serde_json::json!(15002));
        assert_eq!(
            rules[2]["to"][0]["podSelector"]["matchLabels"]["app.kubernetes.io/name"],
            serde_json::json!("openbao")
        );
        assert_eq!(rules[2]["ports"][0]["port"], serde_json::json!(8200));
    }

    #[test]
    fn the_notebook_reaches_trino_as_the_person() {
        let values = effective_values("jupyterhub", Profile::Full, None, None);
        let snippet = values["hub"]["extraConfig"]["10-oidc"]
            .as_str()
            .expect("10-oidc");
        assert!(snippet.contains("c.Authenticator.enable_auth_state = True"));
        assert!(snippet.contains("spawner.environment[\"TRINO_TOKEN\"]"));
        assert!(snippet.contains("REQUESTS_CA_BUNDLE"));
        assert_eq!(
            values["hub"]["extraEnv"]["STARDELT_TRINO_URL"]["valueFrom"]["configMapKeyRef"],
            serde_json::json!({ "name": "stardelt-identity", "key": "trinoUrl", "optional": true })
        );
        let single = &values["singleuser"];
        assert_eq!(
            single["image"]["name"],
            "ghcr.io/stardelt/jupyter-singleuser"
        );
        assert!(
            single["image"]["tag"]
                .as_str()
                .unwrap()
                .contains("@sha256:")
        );
        let volume = &single["storage"]["extraVolumes"][0];
        assert_eq!(volume["secret"]["secretName"], "trino-ca");
        assert_eq!(
            volume["secret"]["items"][0],
            serde_json::json!({"key": "tls.crt", "path": "ca.crt"})
        );
        assert_eq!(
            single["storage"]["extraVolumeMounts"][0]["mountPath"],
            "/etc/stardelt/trino-ca"
        );
        assert_eq!(
            single["networkPolicy"]["egress"][0]["ports"][0],
            serde_json::json!({"port": 8443, "protocol": "TCP"})
        );
    }

    #[test]
    fn notebooks_are_framable_by_nova() {
        let values = effective_values("jupyterhub", Profile::Full, None, None);
        let snippet = values["hub"]["extraConfig"]["10-oidc"]
            .as_str()
            .expect("10-oidc");
        assert!(snippet.contains("frame-ancestors 'self' {_nova}"));
        assert!(snippet.contains("STARDELT_NOVA_ORIGIN"));
        assert!(snippet.contains("\"SameSite\": \"None\""));
        for env in [&values["hub"], &values["singleuser"]] {
            assert_eq!(
                env["extraEnv"]["STARDELT_NOVA_URL"]["valueFrom"]["configMapKeyRef"],
                serde_json::json!({ "name": "stardelt-identity", "key": "publicUrl", "optional": true })
            );
        }
    }

    #[test]
    fn openbao_values_mount_the_seal_key_secret() {
        let values = golden_values("openbao");
        assert_eq!(
            values["server"]["extraVolumes"][0]["name"],
            "openbao-seal-key"
        );
        assert_eq!(values["server"]["extraVolumes"][0]["path"], "/openbao/seal");
        assert_eq!(values["injector"]["enabled"], false);
    }
}
