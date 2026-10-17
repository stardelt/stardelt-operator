//! Pure reconcile planning. Separating the *plan* (which components a resource
//! needs) from the *apply* (server-side apply of upstream charts) keeps the
//! interesting logic unit-testable without a cluster.

use crate::crd::{
    CatalogImpl, IngestionDomain, IngestionSourceSpec, LakehouseSpec, PipelineEngine,
    PipelineSpec, PlatformInstanceSpec, StepKind, StorageImpl,
};

#[derive(Debug, PartialEq, Eq)]
pub struct ComponentPlan {
    /// Stable component name (also the status key).
    pub name: &'static str,
    /// Upstream Helm chart reference, if installed via Helm.
    pub chart: Option<&'static str>,
}

/// Platform-level prerequisites, installed once per `PlatformInstance` (not per
/// Lakehouse). The CNPG operator is cluster-scoped, so it lives here.
pub fn plan_platform(_spec: &PlatformInstanceSpec) -> Vec<ComponentPlan> {
    vec![ComponentPlan {
        name: "cnpg",
        chart: Some("cnpg/cloudnative-pg"),
    }]
}

/// Which components a `Lakehouse` needs, in apply order.
///
/// Storage first (S3 must exist), then the catalog. The CNPG *operator* is a
/// platform-level prerequisite (`plan_platform`); a Lakehouse only owns
/// namespace-scoped resources.
pub fn plan_lakehouse(spec: &LakehouseSpec) -> Vec<ComponentPlan> {
    let mut plan = Vec::new();

    if spec.storage.implementation == StorageImpl::Seaweedfs {
        plan.push(ComponentPlan {
            name: "seaweedfs",
            chart: Some("seaweedfs/seaweedfs"),
        });
    }

    match spec.catalog.implementation {
        CatalogImpl::Lakekeeper => plan.push(ComponentPlan {
            name: "lakekeeper",
            chart: Some("lakekeeper/lakekeeper"),
        }),
        CatalogImpl::Polaris => plan.push(ComponentPlan {
            name: "polaris",
            chart: Some("polaris/polaris"),
        }),
    }

    plan
}

/// Which components an `IngestionSource` needs, in apply order.
///
/// The domain picks the plug (master spec §5, plugs 8–10): SeaTunnel for SaaS
/// connectors, NiFi for enterprise/edge integration, Debezium for CDC. CDC needs
/// Kafka first — Debezium streams into it before anything lands in Iceberg.
pub fn plan_ingestion_source(spec: &IngestionSourceSpec) -> Vec<ComponentPlan> {
    match spec.domain {
        IngestionDomain::Saas => {
            vec![ComponentPlan {
                name: "seatunnel",
                chart: Some("seatunnel/seatunnel"),
            }]
        }
        IngestionDomain::Enterprise => {
            vec![ComponentPlan {
                name: "nifi",
                chart: Some("nifi/nifi"),
            }]
        }
        IngestionDomain::Cdc => vec![
            ComponentPlan {
                name: "kafka",
                chart: Some("strimzi/strimzi-kafka-operator"),
            },
            ComponentPlan {
                name: "debezium",
                chart: Some("debezium/debezium-operator"),
            },
        ],
    }
}

/// Which components a `Pipeline` needs, in apply order.
///
/// The engine selects the orchestrator (Airflow by default, Spark for heavy
/// compute). A dbt step additionally needs dbt-core, which runs as a native
/// step image rather than a Helm release — hence `chart: None`.
pub fn plan_pipeline(spec: &PipelineSpec) -> Vec<ComponentPlan> {
    let mut plan = match spec.engine {
        PipelineEngine::Airflow => {
            vec![ComponentPlan {
                name: "airflow",
                chart: Some("apache-airflow/airflow"),
            }]
        }
        PipelineEngine::Spark => {
            vec![ComponentPlan {
                name: "spark-operator",
                chart: Some("spark-operator/spark-operator"),
            }]
        }
    };

    if spec.steps.iter().any(|step| step.kind == StepKind::Dbt) {
        plan.push(ComponentPlan {
            name: "dbt",
            chart: None,
        });
    }

    plan
}

/// Golden chart versions the operator installs. One place to pin; mirrors the
/// platform Makefile. Bumped by Renovate in the platform repo today.
pub fn chart_version(chart: &str) -> Option<&'static str> {
    match chart {
        "cnpg/cloudnative-pg" => Some("0.28.2"),
        "seaweedfs/seaweedfs" => Some("4.25.1"),
        "lakekeeper/lakekeeper" => Some("0.11.0"),
        "trino/trino" => Some("1.42.2"),
        _ => None,
    }
}

/// Chart repository for a chart reference. Passed to helm as `--repo`, so the
/// operator needs no `helm repo add` state (and no writable cache).
pub fn chart_repo(chart: &str) -> Option<&'static str> {
    match chart {
        "cnpg/cloudnative-pg" => Some("https://cloudnative-pg.github.io/charts"),
        "seaweedfs/seaweedfs" => Some("https://seaweedfs.github.io/seaweedfs/helm"),
        "lakekeeper/lakekeeper" => Some("https://lakekeeper.github.io/lakekeeper-charts/"),
        "trino/trino" => Some("https://trinodb.github.io/charts"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::crd::{CatalogSpec, PipelineStep, PlatformInstanceSpec, StorageSpec};

    fn spec(catalog: CatalogImpl, storage: StorageImpl) -> LakehouseSpec {
        LakehouseSpec {
            catalog: CatalogSpec {
                implementation: catalog,
            },
            storage: StorageSpec {
                implementation: storage,
                bucket: None,
            },
            profile: Default::default(),
            dry_run: true,
            set: Default::default(),
            adopt: false,
        }
    }

    /// Component names of a plan, in apply order.
    fn names(plan: &[ComponentPlan]) -> Vec<&'static str> {
        plan.iter().map(|c| c.name).collect()
    }

    fn ingestion(domain: IngestionDomain) -> IngestionSourceSpec {
        IngestionSourceSpec {
            domain,
            target: "warehouse.raw.events".into(),
            schedule: None,
            source: BTreeMap::new(),
        }
    }

    fn step(name: &str, kind: StepKind) -> PipelineStep {
        PipelineStep {
            name: name.into(),
            kind,
            config: BTreeMap::new(),
        }
    }

    #[test]
    fn default_plan_is_seaweedfs_lakekeeper() {
        let plan = plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs));
        assert_eq!(names(&plan), vec!["seaweedfs", "lakekeeper"]);
    }

    #[test]
    fn platform_plan_owns_the_cluster_scoped_cnpg_operator() {
        let pi = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Default::default(),
            dry_run: true,
        };
        assert_eq!(names(&plan_platform(&pi)), vec!["cnpg"]);
        // It must not leak into a per-Lakehouse plan.
        let lh = plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs));
        assert!(!names(&lh).contains(&"cnpg"));
    }

    #[test]
    fn byos3_skips_seaweedfs_and_polaris_replaces_lakekeeper() {
        let plan = plan_lakehouse(&spec(CatalogImpl::Polaris, StorageImpl::ByoS3));
        assert_eq!(names(&plan), vec!["polaris"]);
    }

    #[test]
    fn every_default_chart_has_a_pinned_version() {
        for c in plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs)) {
            if let Some(chart) = c.chart {
                assert!(chart_version(chart).is_some(), "no pin for {chart}");
            }
        }
    }

    #[test]
    fn saas_ingestion_plans_seatunnel() {
        let plan = plan_ingestion_source(&ingestion(IngestionDomain::Saas));
        assert_eq!(names(&plan), vec!["seatunnel"]);
        assert_eq!(plan[0].chart, Some("seatunnel/seatunnel"));
    }

    #[test]
    fn enterprise_ingestion_plans_nifi() {
        let plan = plan_ingestion_source(&ingestion(IngestionDomain::Enterprise));
        assert_eq!(names(&plan), vec!["nifi"]);
        assert_eq!(plan[0].chart, Some("nifi/nifi"));
    }

    #[test]
    fn cdc_ingestion_plans_kafka_before_debezium() {
        let plan = plan_ingestion_source(&ingestion(IngestionDomain::Cdc));
        assert_eq!(names(&plan), vec!["kafka", "debezium"]);
    }

    #[test]
    fn airflow_pipeline_plans_only_airflow() {
        let spec = PipelineSpec {
            steps: vec![
                step("load", StepKind::Sql),
                step("assert", StepKind::Quality),
            ],
            schedule: Some("0 3 * * *".into()),
            engine: PipelineEngine::Airflow,
        };
        assert_eq!(names(&plan_pipeline(&spec)), vec!["airflow"]);
    }

    #[test]
    fn spark_pipeline_with_dbt_step_plans_spark_operator_and_dbt() {
        let spec = PipelineSpec {
            steps: vec![
                step("build", StepKind::Dbt),
                step("enrich", StepKind::Python),
            ],
            schedule: None,
            engine: PipelineEngine::Spark,
        };
        let plan = plan_pipeline(&spec);
        assert_eq!(names(&plan), vec!["spark-operator", "dbt"]);
        assert_eq!(plan[0].chart, Some("spark-operator/spark-operator"));
        // dbt ships as a step image, not a Helm release.
        assert_eq!(plan[1].chart, None);
    }

    #[test]
    fn sql_only_pipeline_does_not_pull_in_dbt() {
        let spec = PipelineSpec {
            steps: vec![step("load", StepKind::Sql)],
            schedule: None,
            engine: PipelineEngine::Spark,
        };
        assert_eq!(names(&plan_pipeline(&spec)), vec!["spark-operator"]);
    }
}
