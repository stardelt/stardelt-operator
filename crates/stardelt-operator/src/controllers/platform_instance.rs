//! The `PlatformInstance` controller.
//!
//! Reconcile is a declarative, ordered walk of the core-stack dependency DAG
//! (object storage → catalog → query engine → orchestration → BI → UI). Each
//! pass applies every resource idempotently via server-side apply, then gates on
//! the next un-ready step: if a step is not yet ready we record a `False`
//! condition and requeue; once all steps are ready we record top-level `Ready`
//! and resync on a slow interval.
//!
//! The step order mirrors `stardelt-platform/Makefile` + `stardelt-demos/kind/up.sh`.
//! Flux `dependsOn` is also set between HelmReleases as defense-in-depth, but the
//! operator's own gating here is the source of truth — Flux cannot order a
//! HelmRelease behind a native CNPG `Cluster` becoming Ready or a `Job` succeeding.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{Controller, watcher};
use kube::{Api, Client, Resource, ResourceExt};
use serde_json::{Value, json};
use tracing::{error, info, warn};

use crate::api::PlatformInstance;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::readiness::{Condition, condition, ready_condition_true, upsert};
use crate::resources::{self, bootstrap, cnpg, flux, nova, secret};

/// Shared reconcile context.
pub struct Context {
    pub client: Client,
    pub config: Config,
}

/// Entry point: build the controller and run it until the process exits.
pub async fn run(client: Client, config: Config) -> anyhow::Result<()> {
    let api: Api<PlatformInstance> = Api::all(client.clone());
    let ctx = Arc::new(Context { client, config });

    info!("starting PlatformInstance controller");
    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => info!(object = %obj, "reconciled"),
                Err(e) => warn!(error = %e, "reconcile failed"),
            }
        })
        .await;
    Ok(())
}

/// Outcome of a single reconcile step.
enum Gate {
    /// Step's resource is ready; continue to the next step.
    Ready,
    /// Step applied but not yet ready; requeue with the reason recorded.
    Pending(&'static str),
}

async fn reconcile(pi: Arc<PlatformInstance>, ctx: Arc<Context>) -> Result<Action> {
    let name = pi.name_any();
    let ns = pi.spec.namespace.clone();
    let fm = &ctx.config.field_manager;
    let client = &ctx.client;
    let owner = owner_ref(&pi);
    let owner_json = serde_json::to_value(&owner)?;

    info!(name = %name, namespace = %ns, "reconciling PlatformInstance");

    let mut conditions: Vec<Condition> = Vec::new();

    // Step 0: target namespace.
    ensure_namespace(client, fm, &ns).await?;

    // Step 1: Flux HelmRepositories. Gate: all exist (they reconcile fast).
    for repo in flux::REPOS {
        let body = flux::helm_repository(repo, &ns, &owner_json);
        resources::apply_dynamic(
            client,
            fm,
            &flux::helm_repository_gvk(),
            &ns,
            repo.name,
            body,
        )
        .await?;
    }
    record(&mut conditions, "HelmReposReady", true, "Applied");

    // The ordered component steps. Each returns a Gate; the first Pending stops
    // the pass and requeues.
    macro_rules! gate {
        ($cond:expr, $body:expr) => {{
            match $body {
                Gate::Ready => record(&mut conditions, $cond, true, "Ready"),
                Gate::Pending(reason) => {
                    record(&mut conditions, $cond, false, reason);
                    return finish(&ctx, &pi, conditions, false).await;
                }
            }
        }};
    }

    // Step 2: CNPG operator (HelmRelease). CRDs must register before the Cluster.
    gate!(
        "CNPGOperatorReady",
        apply_release(&ctx, &pi, &flux::CNPG, &[], &owner_json).await?
    );

    // Step 3: lakekeeper-pg CNPG Cluster.
    gate!(
        "PostgresReady",
        apply_cnpg_cluster(&ctx, &pi, &owner_json).await?
    );

    // Step 4: SeaweedFS (creates the lakehouse bucket).
    gate!(
        "SeaweedFSReady",
        apply_release(&ctx, &pi, &flux::SEAWEEDFS, &[], &owner_json).await?
    );

    // Step 5: ozone-s3-creds Secret (creds match the SeaweedFS values).
    let sec = secret::build(&pi, owner.clone());
    resources::apply::<Secret>(client, fm, &sec).await?;
    record(&mut conditions, "S3CredentialsReady", true, "Applied");

    // Step 6: Lakekeeper (runs DB migrations; helmWait blocks until done).
    gate!(
        "LakekeeperReady",
        apply_release(&ctx, &pi, &flux::LAKEKEEPER, &["seaweedfs"], &owner_json).await?
    );

    // Step 7: lakekeeper-bootstrap Job (ToS + create `warehouse`).
    gate!("WarehouseReady", apply_bootstrap(&ctx, &pi, &owner).await?);

    // Step 8: Trino.
    gate!(
        "TrinoReady",
        apply_release(&ctx, &pi, &flux::TRINO, &["lakekeeper"], &owner_json).await?
    );

    // Step 9: Airflow (optional).
    if pi.spec.components.airflow_enabled {
        gate!(
            "AirflowReady",
            apply_release(&ctx, &pi, &flux::AIRFLOW, &["lakekeeper"], &owner_json).await?
        );
    }

    // Step 10: Superset (optional).
    if pi.spec.components.superset_enabled {
        gate!(
            "SupersetReady",
            apply_release(&ctx, &pi, &flux::SUPERSET, &["trino"], &owner_json).await?
        );
    }

    // Step 11: Nova Deployment + Service.
    gate!("NovaReady", apply_nova(&ctx, &pi, &owner).await?);

    // All steps ready.
    finish(&ctx, &pi, conditions, true).await
}

// ---------------------------------------------------------------------------
// Step implementations
// ---------------------------------------------------------------------------

/// Apply a Flux HelmRelease and gate on its `Ready` condition.
async fn apply_release(
    ctx: &Context,
    pi: &PlatformInstance,
    chart: &flux::Chart,
    depends_on: &[&str],
    owner_json: &Value,
) -> Result<Gate> {
    let ns = &pi.spec.namespace;
    let body = flux::helm_release(pi, chart, depends_on, owner_json)?;
    resources::apply_dynamic(
        &ctx.client,
        &ctx.config.field_manager,
        &flux::helm_release_gvk(),
        ns,
        chart.release,
        body,
    )
    .await?;

    let current =
        resources::get_dynamic(&ctx.client, &flux::helm_release_gvk(), ns, chart.release).await?;
    Ok(gate_on_ready(current))
}

/// Apply the CNPG Cluster and gate on its `Ready` condition.
async fn apply_cnpg_cluster(
    ctx: &Context,
    pi: &PlatformInstance,
    owner_json: &Value,
) -> Result<Gate> {
    let ns = &pi.spec.namespace;
    let body = cnpg::build(pi, owner_json.clone());
    resources::apply_dynamic(
        &ctx.client,
        &ctx.config.field_manager,
        &cnpg::gvk(),
        ns,
        cnpg::CLUSTER_NAME,
        body,
    )
    .await?;
    let current = resources::get_dynamic(&ctx.client, &cnpg::gvk(), ns, cnpg::CLUSTER_NAME).await?;
    Ok(gate_on_ready(current))
}

/// Apply the bootstrap Job and gate on `status.succeeded >= 1`.
async fn apply_bootstrap(
    ctx: &Context,
    pi: &PlatformInstance,
    owner: &OwnerReference,
) -> Result<Gate> {
    let ns = &pi.spec.namespace;
    let job = bootstrap::build(pi, owner.clone());
    resources::apply::<Job>(&ctx.client, &ctx.config.field_manager, &job).await?;

    let api: Api<Job> = Api::namespaced(ctx.client.clone(), ns);
    let succeeded = api
        .get_opt(bootstrap::JOB_NAME)
        .await?
        .and_then(|j| j.status)
        .and_then(|s| s.succeeded)
        .unwrap_or(0);
    Ok(if succeeded >= 1 {
        Gate::Ready
    } else {
        Gate::Pending("JobRunning")
    })
}

/// Apply Nova Deployment + Service and gate on `availableReplicas >= 1`.
async fn apply_nova(ctx: &Context, pi: &PlatformInstance, owner: &OwnerReference) -> Result<Gate> {
    let ns = &pi.spec.namespace;
    let fm = &ctx.config.field_manager;
    resources::apply::<Deployment>(&ctx.client, fm, &nova::deployment(pi, owner.clone())).await?;
    resources::apply::<Service>(&ctx.client, fm, &nova::service(pi, owner.clone())).await?;

    let api: Api<Deployment> = Api::namespaced(ctx.client.clone(), ns);
    let available = api
        .get_opt(nova::NAME)
        .await?
        .and_then(|d| d.status)
        .and_then(|s| s.available_replicas)
        .unwrap_or(0);
    Ok(if available >= 1 {
        Gate::Ready
    } else {
        Gate::Pending("Starting")
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn gate_on_ready(obj: Option<kube::core::DynamicObject>) -> Gate {
    match obj {
        Some(o) => {
            let v = serde_json::to_value(&o).unwrap_or(Value::Null);
            if ready_condition_true(&v) {
                Gate::Ready
            } else {
                Gate::Pending("NotReady")
            }
        }
        None => Gate::Pending("NotFound"),
    }
}

fn record(conditions: &mut Vec<Condition>, type_: &str, ready: bool, reason: &str) {
    upsert(
        conditions,
        condition(type_, ready, reason, format!("{type_}: {reason}")),
    );
}

/// Patch the final status for this pass and pick a requeue interval.
async fn finish(
    ctx: &Context,
    pi: &PlatformInstance,
    mut conditions: Vec<Condition>,
    all_ready: bool,
) -> Result<Action> {
    let phase = if all_ready {
        "Ready".to_string()
    } else {
        let pending = conditions
            .iter()
            .find(|c| c.status == "False")
            .map(|c| c.type_.clone())
            .unwrap_or_else(|| "Reconciling".to_string());
        format!("Reconciling: {pending}")
    };
    upsert(
        &mut conditions,
        condition(
            "Ready",
            all_ready,
            if all_ready {
                "AllComponentsReady"
            } else {
                "Progressing"
            },
            phase.clone(),
        ),
    );

    patch_status(
        ctx,
        pi,
        json!({
            "phase": phase,
            "ready": if all_ready { "True" } else { "False" },
            "observedGeneration": pi.meta().generation.unwrap_or_default(),
            "conditions": conditions,
        }),
    )
    .await?;

    let secs = if all_ready {
        ctx.config.resync_secs
    } else {
        ctx.config.poll_secs
    };
    Ok(Action::requeue(Duration::from_secs(secs)))
}

fn error_policy(pi: Arc<PlatformInstance>, err: &Error, _ctx: Arc<Context>) -> Action {
    error!(name = %pi.name_any(), error = %err, "requeueing after error");
    Action::requeue(Duration::from_secs(15))
}

/// Build the owner reference for child objects (enables GC + watch requeue).
fn owner_ref(pi: &PlatformInstance) -> OwnerReference {
    OwnerReference {
        api_version: PlatformInstance::api_version(&()).to_string(),
        kind: PlatformInstance::kind(&()).to_string(),
        name: pi.name_any(),
        uid: pi.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Ensure the target namespace exists (SSA is idempotent).
async fn ensure_namespace(client: &Client, fm: &str, ns: &str) -> Result<()> {
    let api: Api<Namespace> = Api::all(client.clone());
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns, "labels": resources::labels() }
    });
    let obj: Namespace = serde_json::from_value(body)?;
    api.patch(ns, &PatchParams::apply(fm).force(), &Patch::Apply(&obj))
        .await?;
    Ok(())
}

/// Merge-patch the `/status` subresource.
async fn patch_status(ctx: &Context, pi: &PlatformInstance, status: Value) -> Result<()> {
    let api: Api<PlatformInstance> = Api::all(ctx.client.clone());
    let patch = json!({
        "apiVersion": PlatformInstance::api_version(&()),
        "kind": PlatformInstance::kind(&()),
        "status": status,
    });
    api.patch_status(
        &pi.name_any(),
        &PatchParams::apply(&ctx.config.field_manager).force(),
        &Patch::Apply(&patch),
    )
    .await?;
    Ok(())
}
