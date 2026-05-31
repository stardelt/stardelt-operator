//! Flux `HelmRepository` and `HelmRelease` builders.
//!
//! The operator does not run Helm itself; it SSA-applies Flux custom resources
//! and lets Flux's source- and helm-controllers do the installs. Chart names,
//! repos, and pinned versions mirror `stardelt-platform/Makefile`. The six
//! `helm-values/*.yaml` are vendored under `src/values/` and embedded with
//! `include_str!` (the platform repo is a separate git repo, so cross-repo
//! `include_str!` is impossible).

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::PlatformInstance;
use crate::api::platform_instance::chart_versions as cv;
use crate::error::Result;

pub fn helm_repository_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("source.toolkit.fluxcd.io", "v1", "HelmRepository")
}

pub fn helm_release_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("helm.toolkit.fluxcd.io", "v2", "HelmRelease")
}

/// One upstream Helm repository.
pub struct Repo {
    pub name: &'static str,
    pub url: &'static str,
}

/// The six repositories, matching `Makefile:_helm-repos`.
pub const REPOS: &[Repo] = &[
    Repo {
        name: "cnpg",
        url: "https://cloudnative-pg.github.io/charts",
    },
    Repo {
        name: "seaweedfs",
        url: "https://seaweedfs.github.io/seaweedfs/helm",
    },
    Repo {
        name: "lakekeeper",
        url: "https://charts.lakekeeper.io",
    },
    Repo {
        name: "trino",
        url: "https://trinodb.github.io/charts",
    },
    Repo {
        name: "apache-airflow",
        url: "https://airflow.apache.org",
    },
    Repo {
        name: "superset",
        url: "https://apache.github.io/superset",
    },
];

/// Build a `HelmRepository` body for the target namespace.
pub fn helm_repository(repo: &Repo, namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "source.toolkit.fluxcd.io/v1",
        "kind": "HelmRepository",
        "metadata": {
            "name": repo.name,
            "namespace": namespace,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": {
            "interval": "1h",
            "url": repo.url,
        }
    })
}

// ---------------------------------------------------------------------------
// Embedded chart values
// ---------------------------------------------------------------------------

const SEAWEEDFS_VALUES: &str = include_str!("../values/seaweedfs.yaml");
const LAKEKEEPER_VALUES: &str = include_str!("../values/lakekeeper.yaml");
const TRINO_VALUES: &str = include_str!("../values/trino.yaml");
const AIRFLOW_VALUES: &str = include_str!("../values/airflow.yaml");
const SUPERSET_VALUES: &str = include_str!("../values/superset.yaml");

/// Identifies a chart and how to build its release.
pub struct Chart {
    pub release: &'static str,
    pub repo: &'static str,
    pub chart: &'static str,
}

pub const CNPG: Chart = Chart {
    release: "cnpg",
    repo: "cnpg",
    chart: "cloudnative-pg",
};
pub const SEAWEEDFS: Chart = Chart {
    release: "seaweedfs",
    repo: "seaweedfs",
    chart: "seaweedfs",
};
pub const LAKEKEEPER: Chart = Chart {
    release: "lakekeeper",
    repo: "lakekeeper",
    chart: "lakekeeper",
};
pub const TRINO: Chart = Chart {
    release: "trino",
    repo: "trino",
    chart: "trino",
};
pub const AIRFLOW: Chart = Chart {
    release: "airflow",
    repo: "apache-airflow",
    chart: "airflow",
};
pub const SUPERSET: Chart = Chart {
    release: "superset",
    repo: "superset",
    chart: "superset",
};

/// Resolve the pinned version for a chart, honoring per-instance overrides.
fn version(pi: &PlatformInstance, release: &str) -> String {
    let ov = pi.spec.chart_versions.as_ref();
    let pick =
        |o: Option<&String>, default: &str| o.cloned().unwrap_or_else(|| default.to_string());
    match release {
        "cnpg" => pick(ov.and_then(|o| o.cnpg.as_ref()), cv::CNPG),
        "seaweedfs" => pick(ov.and_then(|o| o.seaweedfs.as_ref()), cv::SEAWEEDFS),
        "lakekeeper" => pick(ov.and_then(|o| o.lakekeeper.as_ref()), cv::LAKEKEEPER),
        "trino" => pick(ov.and_then(|o| o.trino.as_ref()), cv::TRINO),
        "airflow" => pick(ov.and_then(|o| o.airflow.as_ref()), cv::AIRFLOW),
        "superset" => pick(ov.and_then(|o| o.superset.as_ref()), cv::SUPERSET),
        _ => String::new(),
    }
}

/// Parse an embedded YAML values doc to JSON, substituting the target namespace
/// for the hardcoded `stardelt` in service-DNS references.
fn embedded_values(yaml: &str, namespace: &str) -> Result<Value> {
    let substituted = yaml.replace(
        ".stardelt.svc.cluster.local",
        &format!(".{namespace}.svc.cluster.local"),
    );
    let v: Value = serde_yaml::from_str(&substituted)?;
    Ok(v)
}

/// Recursively merge `overlay` into `base` (overlay wins on scalars/arrays).
fn merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(b), Value::Object(o)) => {
            for (k, v) in o {
                merge(b.entry(k).or_insert(Value::Null), v);
            }
        }
        (b, o) => *b = o,
    }
}

/// Compute the per-instance values for `chart`, merging spec knobs over the
/// embedded defaults.
fn values_for(pi: &PlatformInstance, chart: &Chart) -> Result<Value> {
    let ns = &pi.spec.namespace;
    let s3 = &pi.spec.s3;
    match chart.release {
        // CNPG operator chart takes no custom values in the MVP install.
        "cnpg" => Ok(json!({})),
        "seaweedfs" => {
            let mut v = embedded_values(SEAWEEDFS_VALUES, ns)?;
            merge(
                &mut v,
                json!({
                    "volume": { "dataDirs": [{
                        "name": "data", "type": "persistentVolumeClaim",
                        "size": pi.spec.storage.volume_size, "maxVolumes": 0
                    }]},
                    "s3": { "credentials": { "admin": {
                        "accessKey": s3.access_key, "secretKey": s3.secret_key
                    }}, "createBuckets": [{ "name": s3.bucket, "anonymousRead": false }] }
                }),
            );
            Ok(v)
        }
        "lakekeeper" => embedded_values(LAKEKEEPER_VALUES, ns),
        "trino" => {
            let mut v = embedded_values(TRINO_VALUES, ns)?;
            merge(
                &mut v,
                json!({ "server": { "workers": pi.spec.components.trino.workers } }),
            );
            Ok(v)
        }
        "airflow" => embedded_values(AIRFLOW_VALUES, ns),
        "superset" => embedded_values(SUPERSET_VALUES, ns),
        _ => Ok(json!({})),
    }
}

/// Build a `HelmRelease` body. `depends_on` names other HelmReleases in the
/// same namespace that Flux should order before this one (defense-in-depth;
/// the operator's own step gating is the source of truth).
pub fn helm_release(
    pi: &PlatformInstance,
    chart: &Chart,
    depends_on: &[&str],
    owner: &Value,
) -> Result<Value> {
    let ns = &pi.spec.namespace;
    let deps: Vec<Value> = depends_on.iter().map(|d| json!({ "name": d })).collect();

    Ok(json!({
        "apiVersion": "helm.toolkit.fluxcd.io/v2",
        "kind": "HelmRelease",
        "metadata": {
            "name": chart.release,
            "namespace": ns,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": {
            "interval": "5m",
            "timeout": "10m",
            "releaseName": chart.release,
            "chart": {
                "spec": {
                    "chart": chart.chart,
                    "version": version(pi, chart.release),
                    "sourceRef": {
                        "kind": "HelmRepository",
                        "name": chart.repo,
                        "namespace": ns,
                    }
                }
            },
            "install": { "createNamespace": true },
            "upgrade": { "remediation": { "retries": 3 } },
            "dependsOn": deps,
            "values": values_for(pi, chart)?,
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance() -> PlatformInstance {
        // Round-trip through JSON so all serde defaults are applied, matching
        // what the API server would hand the controller.
        serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": { "namespace": "lake" }
        }))
        .unwrap()
    }

    #[test]
    fn all_embedded_values_parse() {
        for c in [&SEAWEEDFS, &LAKEKEEPER, &TRINO, &AIRFLOW, &SUPERSET] {
            let pi = instance();
            let v = values_for(&pi, c).expect("values_for must succeed");
            assert!(v.is_object(), "{} values should be an object", c.release);
        }
    }

    #[test]
    fn namespace_is_substituted_in_values() {
        let pi = instance();
        let trino = values_for(&pi, &TRINO).unwrap();
        let serialized = serde_json::to_string(&trino).unwrap();
        assert!(
            serialized.contains(".lake.svc.cluster.local"),
            "service DNS should target the spec namespace"
        );
        assert!(
            !serialized.contains(".stardelt.svc.cluster.local"),
            "no references to the hardcoded default namespace should remain"
        );
    }

    #[test]
    fn spec_knobs_override_embedded_defaults() {
        let mut pi = instance();
        pi.spec.components.trino.workers = 7;
        let trino = values_for(&pi, &TRINO).unwrap();
        assert_eq!(trino["server"]["workers"], serde_json::json!(7));
    }

    #[test]
    fn helm_release_builds_with_pinned_version() {
        let pi = instance();
        let owner = serde_json::json!({});
        let hr = helm_release(&pi, &TRINO, &["lakekeeper"], &owner).unwrap();
        assert_eq!(hr["spec"]["chart"]["spec"]["version"], cv::TRINO);
        assert_eq!(hr["spec"]["dependsOn"][0]["name"], "lakekeeper");
    }
}
