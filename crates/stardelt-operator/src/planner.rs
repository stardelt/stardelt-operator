//! Pure reconcile planning. Separating the *plan* (which components a resource
//! needs) from the *apply* (server-side apply of upstream charts) keeps the
//! interesting logic unit-testable without a cluster.

use crate::crd::{
    CatalogImpl, IngestionDomain, IngestionSourceSpec, LakehouseSpec, PipelineEngine, PipelineSpec,
    PlatformInstanceSpec, SecretsMode, StorageImpl, TaskKind,
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
pub fn plan_platform(spec: &PlatformInstanceSpec) -> Vec<ComponentPlan> {
    // Cluster-scoped prerequisites first, then the pillar components that belong
    // to the platform rather than to one lakehouse.
    let mut plan = vec![ComponentPlan {
        name: "cnpg",
        chart: Some("cnpg/cloudnative-pg"),
    }];
    // The secrets contract (decision D23): External Secrets is the one component that talks to a
    // store, so it comes before any component whose pods read a synced Secret. `none` installs
    // nothing; the store itself (platform mode) is planned below.
    if spec.secrets.mode != SecretsMode::None {
        plan.push(ComponentPlan {
            name: "external-secrets",
            chart: Some("external-secrets/external-secrets"),
        });
    }
    // The store itself, only in platform mode (decision D23): External Secrets syncs from it, so it
    // is planned after the operator that talks to it. `external` mode installs no store — the
    // enterprise runs one — and `none` installs neither.
    if spec.secrets.mode == SecretsMode::Platform {
        plan.push(ComponentPlan {
            name: "openbao",
            chart: Some("openbao/openbao"),
        });
    }
    if spec.pillars.notebooks {
        plan.push(ComponentPlan {
            name: "jupyterhub",
            chart: Some("jupyterhub/jupyterhub"),
        });
    }
    if spec.pillars.pipelines {
        plan.push(ComponentPlan {
            name: "airflow",
            chart: Some("apache-airflow/airflow"),
        });
    }
    if spec.pillars.dashboards {
        plan.push(ComponentPlan {
            name: "superset",
            chart: Some("superset/superset"),
        });
    }
    // The streaming bus (D42): Strimzi is the operator, not the broker. A `Kafka` resource
    // is applied separately once the pillar is on, so a cluster that does not stream pays
    // for nothing but the controller it asked for.
    if spec.pillars.streaming {
        plan.push(ComponentPlan {
            name: "kafka",
            chart: Some("strimzi/strimzi-kafka-operator"),
        });
        // The engine of a `StreamApp` (D42): Spark Structured Streaming runs as a
        // `SparkApplication`, so the pillar that streams also owns the operator that reconciles it.
        plan.push(ComponentPlan {
            name: "spark-operator",
            chart: Some("spark-operator/spark-operator"),
        });
    }
    plan
}

/// Which components a `Lakehouse` needs, in apply order.
///
/// Storage first (S3 must exist), then the catalog, then query serving — each
/// stage depends on the one before it. The CNPG *operator* is a platform-level
/// prerequisite (`plan_platform`); a Lakehouse only owns namespace-scoped
/// resources.
pub fn plan_lakehouse(spec: &LakehouseSpec) -> Vec<ComponentPlan> {
    let mut plan = Vec::new();

    if spec.storage.implementation == StorageImpl::Seaweedfs {
        plan.push(ComponentPlan {
            name: "seaweedfs",
            chart: Some("seaweedfs/seaweedfs"),
        });
    }

    // The permission store of the catalog (O5): installed before the catalog that will use
    // it at the cut-over, independent of which catalog implementation is chosen.
    plan.push(ComponentPlan {
        name: "openfga",
        chart: Some("openfga/openfga"),
    });

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

    // Query serving (master spec §5, plug 4): Trino is the default Iceberg REST
    // client and part of the managed Lakehouse.
    plan.push(ComponentPlan {
        name: "trino",
        chart: Some("trino/trino"),
    });

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

    if spec.tasks.iter().any(|task| task.kind == TaskKind::Dbt) {
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
        "trino/trino" => Some("1.45.0"),
        "jupyterhub/jupyterhub" => Some("4.4.5"),
        "apache-airflow/airflow" => Some("1.21.0"),
        "superset/superset" => Some("0.15.5"),
        "openfga/openfga" => Some("0.2.62"),
        "external-secrets/external-secrets" => Some("2.13.0"),
        "openbao/openbao" => Some("0.30.4"),
        "strimzi/strimzi-kafka-operator" => Some("1.3.1"),
        "spark-operator/spark-operator" => Some("2.5.2"),
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
        "jupyterhub/jupyterhub" => Some("https://jupyterhub.github.io/helm-chart/"),
        "apache-airflow/airflow" => Some("https://airflow.apache.org"),
        "superset/superset" => Some("https://apache.github.io/superset"),
        "openfga/openfga" => Some("https://openfga.github.io/helm-charts"),
        "external-secrets/external-secrets" => Some("https://charts.external-secrets.io"),
        "openbao/openbao" => Some("https://openbao.github.io/openbao-helm"),
        "strimzi/strimzi-kafka-operator" => Some("https://strimzi.io/charts/"),
        "spark-operator/spark-operator" => Some("https://kubeflow.github.io/spark-operator"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::crd::{
        CatalogSpec, Pillars, PipelineConcurrency, PipelinePublish, PipelineTask,
        PlatformInstanceSpec, SecretsMode, SecretsSpec, StorageSpec,
    };

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
            component_values: Default::default(),
            adopt: false,
            cluster_operation: Default::default(),
            deletion_policy: Default::default(),
            compute: Default::default(),
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

    fn step(name: &str, kind: TaskKind) -> PipelineTask {
        PipelineTask {
            name: name.into(),
            kind,
            depends_on: vec![],
            sql: (kind == TaskKind::Sql).then(|| "SELECT 1".to_string()),
            warehouse: None,
            connection: None,
            config: BTreeMap::new(),
            writes: Vec::new(),
            checks: Vec::new(),
        }
    }

    fn pipeline(tasks: Vec<PipelineTask>, engine: PipelineEngine) -> PipelineSpec {
        PipelineSpec {
            tasks,
            schedule: None,
            run_as: None,
            owner_group: None,
            viewers: Vec::new(),
            params: BTreeMap::new(),
            retries: None,
            timeout: None,
            concurrency: PipelineConcurrency::default(),
            engine,
            publish: PipelinePublish::Direct,
        }
    }

    #[test]
    fn default_plan_is_storage_then_catalog_then_query() {
        let plan = plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs));
        assert_eq!(
            names(&plan),
            vec!["seaweedfs", "openfga", "lakekeeper", "trino"]
        );
    }

    #[test]
    fn notebooks_pillar_adds_jupyterhub_to_the_platform_plan() {
        let without = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        assert!(!without.pillars.notebooks, "notebooks stay opt-in");
        assert_eq!(names(&plan_platform(&without)), vec!["cnpg"]);

        let with = PlatformInstanceSpec {
            pillars: Pillars {
                notebooks: true,
                ..Default::default()
            },
            ..without
        };
        assert_eq!(names(&plan_platform(&with)), vec!["cnpg", "jupyterhub"]);
    }

    #[test]
    fn secrets_none_plans_no_external_secrets() {
        let base = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        assert_eq!(names(&plan_platform(&base)), vec!["cnpg"]);
    }

    #[test]
    fn external_secrets_comes_before_the_pillars() {
        let base = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        let plan_external = plan_platform(&PlatformInstanceSpec {
            secrets: SecretsSpec {
                mode: SecretsMode::External,
                external: None,
                groups: Vec::new(),
            },
            pillars: Pillars {
                notebooks: true,
                ..Default::default()
            },
            ..base.clone()
        });
        assert_eq!(
            names(&plan_external),
            vec!["cnpg", "external-secrets", "jupyterhub"]
        );
        let plan_platform_mode = plan_platform(&PlatformInstanceSpec {
            secrets: SecretsSpec {
                mode: SecretsMode::Platform,
                external: None,
                groups: Vec::new(),
            },
            pillars: Pillars {
                notebooks: true,
                ..Default::default()
            },
            ..base
        });
        assert_eq!(
            names(&plan_platform_mode),
            vec!["cnpg", "external-secrets", "openbao", "jupyterhub"]
        );
    }

    #[test]
    fn external_secrets_is_pinned_at_the_platform_version_and_repo() {
        let chart = "external-secrets/external-secrets";
        assert_eq!(chart_version(chart), Some("2.13.0"));
        assert_eq!(
            chart_repo(chart),
            Some("https://charts.external-secrets.io")
        );
    }

    #[test]
    fn platform_mode_plans_the_store_after_external_secrets() {
        let base = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: SecretsSpec {
                mode: SecretsMode::Platform,
                external: None,
                groups: Vec::new(),
            },
            ingress: Default::default(),
        };
        assert_eq!(
            names(&plan_platform(&base)),
            vec!["cnpg", "external-secrets", "openbao"]
        );
    }

    #[test]
    fn external_mode_plans_no_store() {
        let base = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: SecretsSpec {
                mode: SecretsMode::External,
                external: None,
                groups: Vec::new(),
            },
            ingress: Default::default(),
        };
        assert_eq!(
            names(&plan_platform(&base)),
            vec!["cnpg", "external-secrets"]
        );
    }

    #[test]
    fn openbao_is_pinned_at_the_platform_version_and_repo() {
        assert_eq!(chart_version("openbao/openbao"), Some("0.30.4"));
        assert_eq!(
            chart_repo("openbao/openbao"),
            Some("https://openbao.github.io/openbao-helm")
        );
    }

    #[test]
    fn pipelines_pillar_adds_airflow_to_the_platform_plan() {
        let base = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        assert!(!base.pillars.pipelines, "pipelines stay opt-in");
        let plan = plan_platform(&PlatformInstanceSpec {
            pillars: Pillars {
                pipelines: true,
                ..Default::default()
            },
            ..base.clone()
        });
        assert_eq!(names(&plan), vec!["cnpg", "airflow"]);
        assert_eq!(plan[1].chart, Some("apache-airflow/airflow"));
        let both = plan_platform(&PlatformInstanceSpec {
            pillars: Pillars {
                notebooks: true,
                pipelines: true,
                ..Default::default()
            },
            ..base
        });
        assert_eq!(names(&both), vec!["cnpg", "jupyterhub", "airflow"]);
    }

    #[test]
    fn airflow_is_pinned_at_the_version_and_repo_of_the_platform() {
        assert_eq!(chart_version("apache-airflow/airflow"), Some("1.21.0"));
        assert_eq!(
            chart_repo("apache-airflow/airflow"),
            Some("https://airflow.apache.org")
        );
    }

    #[test]
    fn dashboards_pillar_adds_superset_to_the_platform_plan() {
        let base = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        assert!(!base.pillars.dashboards, "dashboards stay opt-in");
        let plan = plan_platform(&PlatformInstanceSpec {
            pillars: Pillars {
                dashboards: true,
                ..Default::default()
            },
            ..base.clone()
        });
        assert_eq!(names(&plan), vec!["cnpg", "superset"]);
        assert_eq!(plan[1].chart, Some("superset/superset"));
        let both = plan_platform(&PlatformInstanceSpec {
            pillars: Pillars {
                pipelines: true,
                dashboards: true,
                ..Default::default()
            },
            ..base
        });
        assert_eq!(names(&both), vec!["cnpg", "airflow", "superset"]);
    }

    #[test]
    fn streaming_pillar_plans_the_kafka_operator() {
        let with = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars {
                streaming: true,
                ..Default::default()
            },
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        let plan = plan_platform(&with);
        let kafka = plan
            .iter()
            .find(|c| c.name == "kafka")
            .expect("streaming plans kafka");
        assert_eq!(kafka.chart, Some("strimzi/strimzi-kafka-operator"));

        let without = PlatformInstanceSpec {
            pillars: Pillars {
                streaming: false,
                ..Default::default()
            },
            ..with
        };
        assert!(!names(&plan_platform(&without)).contains(&"kafka"));

        assert_eq!(
            chart_version("strimzi/strimzi-kafka-operator"),
            Some("1.3.1")
        );
        assert_eq!(names(&plan), vec!["cnpg", "kafka", "spark-operator"]);
        assert_eq!(
            chart_version("spark-operator/spark-operator"),
            Some("2.5.2")
        );
        assert_eq!(
            chart_repo("spark-operator/spark-operator"),
            Some("https://kubeflow.github.io/spark-operator")
        );
    }

    #[test]
    fn platform_plan_owns_the_cluster_scoped_cnpg_operator() {
        let pi = PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Default::default(),
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        };
        assert_eq!(names(&plan_platform(&pi)), vec!["cnpg"]);
        // It must not leak into a per-Lakehouse plan.
        let lh = plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs));
        assert!(!names(&lh).contains(&"cnpg"));
    }

    #[test]
    fn byos3_skips_seaweedfs_and_polaris_replaces_lakekeeper() {
        let plan = plan_lakehouse(&spec(CatalogImpl::Polaris, StorageImpl::ByoS3));
        assert_eq!(names(&plan), vec!["openfga", "polaris", "trino"]);
    }

    #[test]
    fn the_lakehouse_plan_installs_openfga_before_the_catalog() {
        assert_eq!(
            names(&plan_lakehouse(&spec(
                CatalogImpl::Lakekeeper,
                StorageImpl::Seaweedfs
            ))),
            vec!["seaweedfs", "openfga", "lakekeeper", "trino"]
        );
    }

    #[test]
    fn every_default_chart_has_a_pinned_version() {
        let platform = plan_platform(&PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars {
                notebooks: true,
                pipelines: true,
                streaming: true,
                ..Default::default()
            },
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        });
        for c in plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs))
            .into_iter()
            .chain(platform)
        {
            if let Some(chart) = c.chart {
                assert!(chart_version(chart).is_some(), "no pin for {chart}");
            }
        }
    }

    #[test]
    fn every_planned_chart_has_a_repo() {
        let platform = plan_platform(&PlatformInstanceSpec {
            profile: Default::default(),
            pillars: Pillars {
                notebooks: true,
                pipelines: true,
                ..Default::default()
            },
            dry_run: true,
            cluster_operation: Default::default(),
            component_values: Default::default(),
            namespace: "stardelt".into(),
            base_domain: None,
            identity: Default::default(),
            secrets: Default::default(),
            ingress: Default::default(),
        });
        for c in plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs))
            .into_iter()
            .chain(platform)
        {
            if let Some(chart) = c.chart {
                assert!(chart_repo(chart).is_some(), "no repo for {chart}");
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
        let spec = pipeline(
            vec![
                step("load", TaskKind::Sql),
                step("assert", TaskKind::Quality),
            ],
            PipelineEngine::Airflow,
        );
        assert_eq!(names(&plan_pipeline(&spec)), vec!["airflow"]);
    }

    #[test]
    fn spark_pipeline_with_dbt_step_plans_spark_operator_and_dbt() {
        let spec = pipeline(
            vec![
                step("build", TaskKind::Dbt),
                step("enrich", TaskKind::Python),
            ],
            PipelineEngine::Spark,
        );
        let plan = plan_pipeline(&spec);
        assert_eq!(names(&plan), vec!["spark-operator", "dbt"]);
        assert_eq!(plan[0].chart, Some("spark-operator/spark-operator"));
        // dbt ships as a step image, not a Helm release.
        assert_eq!(plan[1].chart, None);
    }

    #[test]
    fn sql_only_pipeline_does_not_pull_in_dbt() {
        let spec = pipeline(vec![step("load", TaskKind::Sql)], PipelineEngine::Spark);
        assert_eq!(names(&plan_pipeline(&spec)), vec!["spark-operator"]);
    }
}
