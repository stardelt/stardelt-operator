//! Controller runtime.
//!
//! Watches the stardelt CRDs and reconciles them into real resources:
//! - `Tenant` → a Namespace (the isolation boundary)
//! - `PlatformInstance` → platform-level prerequisites (CNPG operator), once
//! - `Lakehouse` → its namespace-scoped plan (storage + catalog)
//!
//! Applies go through Helm. Both resources default to `dryRun: true`, so a new
//! resource plans first and changes nothing until explicitly flipped.

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
    ComponentStatus, ComponentValues, Lakehouse, LakehouseStatus, PlatformInstance,
    PlatformInstanceStatus, Tenant, TenantStatus,
};
use crate::helm::{CommandHelmRunner, HelmRelease, HelmRunner};
use crate::planner::{ComponentPlan, chart_repo, chart_version, plan_lakehouse, plan_platform};
use crate::values::effective_values;

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

/// Overrides keyed by a component the plan does not contain are almost always a
/// typo (`polaris` while the catalog is Lakekeeper, say); log them loudly.
fn warn_unknown_overrides(plan: &[ComponentPlan], overrides: &ComponentValues) {
    let planned: std::collections::BTreeSet<&str> = plan.iter().map(|c| c.name).collect();
    let named = overrides
        .values
        .keys()
        .map(|k| (k.as_str(), "values"))
        .chain(overrides.set.keys().map(|k| (k.as_str(), "set")));
    for (component, field) in named {
        if !planned.contains(component) {
            tracing::warn!(
                component,
                field,
                "value override names no planned component; ignored"
            );
        }
    }
}

/// Apply (or plan) a component plan, returning a truthful per-component status.
async fn apply_plan(
    ctx: &Context,
    namespace: &str,
    plan: &[ComponentPlan],
    overrides: &ComponentValues,
    dry_run: bool,
    adopt: bool,
) -> Vec<ComponentStatus> {
    warn_unknown_overrides(plan, overrides);

    let mut components = Vec::with_capacity(plan.len());
    for component in plan {
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
            namespace: namespace.to_string(),
            dry_run,
            set: overrides
                .set
                .get(component.name)
                .cloned()
                .unwrap_or_default(),
            values: Some(effective_values(
                component.name,
                overrides.values.get(component.name),
            )),
        };

        // Adopt guard: never silently take over a release this operator did not
        // create. `spec.adopt: true` is the explicit opt-in.
        if !dry_run && !adopt && ctx.helm.run(release.status_args(), None).await.is_ok() {
            components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: false,
                message: Some(
                    "release exists and is not owned by this operator — set spec.adopt: true to take it over"
                        .into(),
                ),
            });
            continue;
        }

        match ctx.helm.run(release.args(), release.values_yaml()).await {
            Ok(_) => components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: !dry_run,
                message: dry_run.then(|| "planned (dry-run)".into()),
            }),
            Err(e) => {
                tracing::warn!(component = component.name, error = %e, "apply failed");
                components.push(ComponentStatus {
                    name: component.name.to_string(),
                    ready: false,
                    message: Some(e.to_string()),
                });
            }
        }
    }
    components
}

fn phase(dry_run: bool, components: &[ComponentStatus]) -> &'static str {
    if dry_run {
        "Planned"
    } else if components.iter().all(|c| c.ready) {
        "Ready"
    } else {
        "Progressing"
    }
}

async fn reconcile_lakehouse(lh: Arc<Lakehouse>, ctx: Arc<Context>) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let namespace = lh.namespace().unwrap_or_else(|| "default".into());
    let plan = plan_lakehouse(&lh.spec);
    let components = apply_plan(
        &ctx,
        &namespace,
        &plan,
        &lh.spec.component_values,
        lh.spec.dry_run,
        lh.spec.adopt,
    )
    .await;
    let phase = phase(lh.spec.dry_run, &components);

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
    tracing::info!(lakehouse = %name, phase, dry_run = lh.spec.dry_run, "reconciled");
    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn reconcile_platform(
    instance: Arc<PlatformInstance>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let name = instance.name_any();
    let plan = plan_platform(&instance.spec);
    let components = apply_plan(
        &ctx,
        "stardelt",
        &plan,
        &instance.spec.component_values,
        instance.spec.dry_run,
        false,
    )
    .await;
    let phase = phase(instance.spec.dry_run, &components);

    let status = PlatformInstanceStatus {
        phase: phase.into(),
        conditions: vec![],
        components,
    };
    let api: Api<PlatformInstance> = Api::all(ctx.client.clone());
    api.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    tracing::info!(instance = %name, phase, dry_run = instance.spec.dry_run, "reconciled");
    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy<K>(_obj: Arc<K>, err: &kube::Error, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile failed");
    Action::requeue(Duration::from_secs(30))
}
