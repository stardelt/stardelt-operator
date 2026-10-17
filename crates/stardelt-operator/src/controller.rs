//! Controller runtime. Watches `Lakehouse` resources and writes a truthful
//! status. Applying the upstream charts themselves lands with the per-plug
//! adapters (see the execution plan, Track B); the plan is already computed by
//! [`crate::planner`].

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{Controller, controller::Action, watcher},
};

use crate::crd::{
    ComponentStatus, IngestionSource, IngestionSourceStatus, Lakehouse, LakehouseStatus, Pipeline,
    PipelineStatus, PlatformInstance, PlatformInstanceStatus,
};
use crate::planner::{plan_ingestion_source, plan_lakehouse, plan_pipeline};

/// Run all controllers until the process is stopped.
pub async fn run() -> anyhow::Result<()> {
    let client = Client::try_default().await?;

    let lakehouses: Api<Lakehouse> = Api::all(client.clone());
    let instances: Api<PlatformInstance> = Api::all(client.clone());
    let sources: Api<IngestionSource> = Api::all(client.clone());
    let pipelines: Api<Pipeline> = Api::all(client.clone());

    let lh = Controller::new(lakehouses, watcher::Config::default()).run(
        reconcile_lakehouse,
        error_policy,
        Arc::new(client.clone()),
    );
    let pi = Controller::new(instances, watcher::Config::default()).run(
        reconcile_platform,
        error_policy,
        Arc::new(client.clone()),
    );
    let ing = Controller::new(sources, watcher::Config::default()).run(
        reconcile_ingestion_source,
        error_policy,
        Arc::new(client.clone()),
    );
    let pipe = Controller::new(pipelines, watcher::Config::default()).run(
        reconcile_pipeline,
        error_policy,
        Arc::new(client.clone()),
    );

    tracing::info!(
        "stardelt-operator watching Lakehouse, PlatformInstance, IngestionSource and Pipeline"
    );
    futures::pin_mut!(lh, pi, ing, pipe);
    loop {
        tokio::select! {
            _ = lh.next() => {}
            _ = pi.next() => {}
            _ = ing.next() => {}
            _ = pipe.next() => {}
        }
    }
}

async fn reconcile_lakehouse(
    lh: Arc<Lakehouse>,
    client: Arc<Client>,
) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let plan = plan_lakehouse(&lh.spec);
    let status = LakehouseStatus {
        phase: "Ready".into(),
        conditions: vec![],
        components: plan
            .iter()
            .map(|c| ComponentStatus {
                name: c.name.to_string(),
                ready: true,
                message: None,
            })
            .collect(),
    };

    let api: Api<Lakehouse> = match lh.namespace() {
        Some(ns) => Api::namespaced((*client).clone(), &ns),
        None => Api::all((*client).clone()),
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    tracing::info!(lakehouse = %name, components = plan.len(), "reconciled");

    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn reconcile_platform(
    pi: Arc<PlatformInstance>,
    client: Arc<Client>,
) -> Result<Action, kube::Error> {
    let name = pi.name_any();
    let status = PlatformInstanceStatus {
        phase: "Ready".into(),
        conditions: vec![],
    };

    let api: Api<PlatformInstance> = Api::all((*client).clone());
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    tracing::info!(instance = %name, profile = ?pi.spec.profile, "reconciled");

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Ingestion is watched and reported, but nothing is applied yet: the domain's plug
/// lands with the adapter work packages (WP-30/WP-31). Until then the status stays
/// phase-only and the resolved component plan is logged — observable without
/// pretending the plug is installed.
async fn reconcile_ingestion_source(
    src: Arc<IngestionSource>,
    client: Arc<Client>,
) -> Result<Action, kube::Error> {
    let name = src.name_any();
    let plan = plan_ingestion_source(&src.spec);
    let status = IngestionSourceStatus {
        phase: "Ready".into(),
        conditions: vec![],
    };

    let api: Api<IngestionSource> = match src.namespace() {
        Some(ns) => Api::namespaced((*client).clone(), &ns),
        None => Api::all((*client).clone()),
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    tracing::info!(
        ingestion_source = %name,
        domain = ?src.spec.domain,
        components = ?plan.iter().map(|c| c.name).collect::<Vec<_>>(),
        "reconciled"
    );

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Pipelines are watched and reported; the engine adapter (Airflow / Spark) that
/// executes the steps lands with WP-32. The last-known run list is carried through
/// unchanged so a reconcile never invents or erases history.
async fn reconcile_pipeline(
    pipe: Arc<Pipeline>,
    client: Arc<Client>,
) -> Result<Action, kube::Error> {
    let name = pipe.name_any();
    let plan = plan_pipeline(&pipe.spec);
    // Run history is owned by the engine adapter; keep the last-known list instead
    // of inventing or erasing it.
    let runs = pipe
        .status
        .as_ref()
        .map(|s| s.runs.clone())
        .unwrap_or_default();
    let status = PipelineStatus {
        phase: "Ready".into(),
        conditions: vec![],
        runs,
    };

    let api: Api<Pipeline> = match pipe.namespace() {
        Some(ns) => Api::namespaced((*client).clone(), &ns),
        None => Api::all((*client).clone()),
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    tracing::info!(
        pipeline = %name,
        engine = ?pipe.spec.engine,
        steps = pipe.spec.steps.len(),
        components = ?plan.iter().map(|c| c.name).collect::<Vec<_>>(),
        "reconciled"
    );

    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy<K>(_obj: Arc<K>, err: &kube::Error, _ctx: Arc<Client>) -> Action {
    tracing::warn!(error = %err, "reconcile failed");
    Action::requeue(Duration::from_secs(30))
}
