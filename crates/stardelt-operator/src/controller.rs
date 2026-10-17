//! Controller runtime.
//!
//! Watches the stardelt CRDs and reconciles them into real resources:
//! - `Tenant` → a Namespace (the isolation boundary)
//! - `Lakehouse` → the component plan, applied through Helm (or planned only,
//!   which is the safe default: `spec.dryRun: true`)
//! - `PlatformInstance` → status (fan-out to child CRDs lands with WP-23)

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::{api::core::v1::Namespace, apimachinery::pkg::apis::meta::v1::ObjectMeta};
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams, PostParams},
    runtime::{Controller, controller::Action, watcher},
};
use serde_json::json;

use crate::crd::{
    ComponentStatus, Lakehouse, LakehouseStatus, PlatformInstance, PlatformInstanceStatus, Tenant,
    TenantStatus,
};
use crate::helm::{CommandHelmRunner, HelmRelease, HelmRunner};
use crate::planner::{chart_repo, chart_version, plan_lakehouse};

pub struct Context {
    pub client: Client,
    pub helm: Arc<dyn HelmRunner>,
}

pub async fn run() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let ctx = Arc::new(Context {
        client: client.clone(),
        helm: Arc::new(CommandHelmRunner),
    });

    let lakehouses = Controller::new(
        Api::<Lakehouse>::all(client.clone()),
        watcher::Config::default(),
    )
    .run(reconcile_lakehouse, error_policy, ctx.clone());
    let tenants = Controller::new(
        Api::<Tenant>::all(client.clone()),
        watcher::Config::default(),
    )
    .run(reconcile_tenant, error_policy, ctx.clone());
    let platforms = Controller::new(
        Api::<PlatformInstance>::all(client.clone()),
        watcher::Config::default(),
    )
    .run(reconcile_platform, error_policy, ctx.clone());

    tracing::info!("stardelt-operator watching PlatformInstance, Tenant and Lakehouse");
    futures::pin_mut!(lakehouses, tenants, platforms);
    loop {
        tokio::select! {
            _ = lakehouses.next() => {}
            _ = tenants.next() => {}
            _ = platforms.next() => {}
        }
    }
}

/// A Tenant owns its Namespace.
async fn reconcile_tenant(tenant: Arc<Tenant>, ctx: Arc<Context>) -> Result<Action, kube::Error> {
    let name = tenant.name_any();
    let namespaces: Api<Namespace> = Api::all(ctx.client.clone());

    if namespaces.get_opt(&name).await?.is_none() {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("stardelt.io/tenant".to_string(), name.clone());
        let namespace = Namespace {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            ..Default::default()
        };
        match namespaces.create(&PostParams::default(), &namespace).await {
            Ok(_) => tracing::info!(tenant = %name, "namespace created"),
            // Lost a race with another reconcile: that is fine.
            Err(kube::Error::Api(e)) if e.code == 409 => {}
            Err(e) => return Err(e),
        }
    }

    // Status must be patched through the namespaced API: `Api::all` has no
    // status endpoint for a namespaced resource (it 404s).
    let api: Api<Tenant> = Api::namespaced(
        ctx.client.clone(),
        tenant.namespace().as_deref().unwrap_or("default"),
    );
    let status = TenantStatus {
        phase: "Ready".into(),
        conditions: vec![],
    };
    api.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(300)))
}

/// A Lakehouse becomes its component plan.
async fn reconcile_lakehouse(lh: Arc<Lakehouse>, ctx: Arc<Context>) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let namespace = lh.namespace().unwrap_or_else(|| "default".into());
    let dry_run = lh.spec.dry_run;
    let plan = plan_lakehouse(&lh.spec);

    let mut components = Vec::with_capacity(plan.len());
    for component in &plan {
        let Some(chart) = component.chart else {
            components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: false,
                message: Some("no adapter yet".into()),
            });
            continue;
        };
        let release = HelmRelease {
            name: component.name.to_string(),
            chart: chart.to_string(),
            repo: chart_repo(chart).map(str::to_string),
            version: chart_version(chart).map(str::to_string),
            namespace: namespace.clone(),
            dry_run,
        };
        match ctx.helm.run(release.args()).await {
            Ok(_) => components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: !dry_run,
                message: dry_run.then(|| "planned (dry-run)".into()),
            }),
            Err(e) => {
                tracing::warn!(lakehouse = %name, component = component.name, error = %e, "apply failed");
                components.push(ComponentStatus {
                    name: component.name.to_string(),
                    ready: false,
                    message: Some(e.to_string()),
                });
            }
        }
    }

    let all_ready = !dry_run && components.iter().all(|c| c.ready);
    let phase = if dry_run {
        "Planned"
    } else if all_ready {
        "Ready"
    } else {
        "Progressing"
    };

    let status = LakehouseStatus {
        phase: phase.into(),
        conditions: vec![],
        components,
    };
    let api: Api<Lakehouse> = match lh.namespace() {
        Some(ns) => Api::namespaced(ctx.client.clone(), &ns),
        None => Api::all(ctx.client.clone()),
    };
    api.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    tracing::info!(lakehouse = %name, phase, dry_run, "reconciled");

    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn reconcile_platform(
    instance: Arc<PlatformInstance>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let name = instance.name_any();
    let status = PlatformInstanceStatus {
        phase: "Ready".into(),
        conditions: vec![],
    };

    let api: Api<PlatformInstance> = Api::all(ctx.client.clone());
    api.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    tracing::info!(instance = %name, profile = ?instance.spec.profile, "reconciled");

    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy<K>(_obj: Arc<K>, err: &kube::Error, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile failed");
    Action::requeue(Duration::from_secs(30))
}
