//! Controller runtime.
//!
//! Watches the stardelt CRDs and reconciles them into real resources:
//! - `PlatformInstance` → platform-level prerequisites (CNPG operator), once
//! - `Lakehouse` → its namespace-scoped plan (storage + catalog)
//! - `Connection` → an External Secrets sync (see [`crate::connection`])
//! - `ServicePrincipal` → validation and its credentials Secret check (see [`crate::principal`])
//!
//! Applies go through Helm. Both resources default to `dryRun: true`, so a new
//! resource plans first and changes nothing until explicitly flipped.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{ConfigMap, PersistentVolumeClaim, Secret},
};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams},
    runtime::{Controller, controller::Action, watcher},
};
use serde_json::json;

use crate::connection::reconcile_connection;
use crate::crd::Profile;
use crate::crd::{
    ComponentStatus, ComponentValues, Condition, Connection, DeletionPolicy, Lakehouse,
    LakehouseStatus, PlatformInstance, PlatformInstanceStatus, ServicePrincipal, StorageImpl,
    ValuesSource,
};
use crate::helm::{CommandHelmRunner, HelmRelease, HelmRunner, is_deployed, is_owned};
use crate::objects::cnpg_cluster_resource;
use crate::planner::{ComponentPlan, chart_repo, chart_version, plan_lakehouse, plan_platform};
use crate::prerequisites::CATALOG_CLUSTER;
use crate::principal::reconcile_principal;
use crate::provision::{
    ensure_bootstrap, ensure_catalog_database, ensure_discovery, ensure_identity,
    ensure_identity_secrets, ensure_keycloak, ensure_keycloak_pg, ensure_s3_secrets,
};
use crate::values::{effective_values, parse_values_document};

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
    let platforms = Controller::new(
        Api::<PlatformInstance>::all(client.clone()),
        watcher::Config::default(),
    )
    .run(reconcile_platform, error_policy, ctx.clone());

    let connections = Controller::new(
        Api::<Connection>::all(client.clone()),
        watcher::Config::default(),
    )
    .run(reconcile_connection, error_policy, ctx.clone());

    let principals = Controller::new(
        Api::<ServicePrincipal>::all(client.clone()),
        watcher::Config::default(),
    )
    .run(reconcile_principal, error_policy, ctx.clone());

    tracing::info!(
        "stardelt-operator watching PlatformInstance, Lakehouse, Connection and ServicePrincipal"
    );
    futures::pin_mut!(lakehouses, platforms, connections, principals);
    loop {
        tokio::select! {
            _ = lakehouses.next() => {}
            _ = platforms.next() => {}
            _ = connections.next() => {}
            _ = principals.next() => {}
        }
    }
}

/// The Ready condition for a phase. Keeps the previous transition time while the status is unchanged.
fn ready_condition(
    phase: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
    now: &str,
) -> Condition {
    let status = if phase == "Ready" { "True" } else { "False" };
    let last_transition_time = match previous {
        Some(p) if p.status == status => p.last_transition_time.clone(),
        _ => Some(now.to_string()),
    };
    Condition {
        type_: "Ready".into(),
        status: status.into(),
        reason: Some(phase.into()),
        message: None,
        last_transition_time,
        observed_generation: generation,
    }
}

fn now_rfc3339() -> String {
    k8s_openapi::jiff::Timestamp::now().to_string()
}

/// Per-component value overrides for one resource, plus the namespace that
/// `valuesFrom` references default to (`None` for cluster-scoped resources,
/// which have no namespace of their own).
struct OverridesContext<'a> {
    scope_namespace: Option<&'a str>,
    overrides: &'a ComponentValues,
    profile: Profile,
}

/// Outcome of applying (or planning) one component plan.
struct PlanOutcome {
    components: Vec<ComponentStatus>,
    /// False when any component's values could not be resolved. That component
    /// is reported in `components` with the reason; the rest of the plan still
    /// proceeds, so one broken reference never blocks the whole Lakehouse.
    values_resolved: bool,
}

/// Overrides keyed by a component the plan does not contain are almost always a
/// typo (`polaris` while the catalog is Lakekeeper, say); log them loudly.
fn warn_unknown_overrides(plan: &[ComponentPlan], overrides: &ComponentValues) {
    let planned: std::collections::BTreeSet<&str> = plan.iter().map(|c| c.name).collect();
    let named = overrides
        .values
        .keys()
        .map(|k| (k.as_str(), "values"))
        .chain(overrides.set.keys().map(|k| (k.as_str(), "set")))
        .chain(
            overrides
                .values_from
                .keys()
                .map(|k| (k.as_str(), "valuesFrom")),
        );
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

/// Resolve the raw values document referenced by `spec.valuesFrom.<component>`.
///
/// Nothing is cached: the document is read on every reconcile, so a ConfigMap or
/// Secret that changed in GitOps is picked up on the next requeue — and a
/// document that cannot be read is never silently replaced by stale or default
/// values.
async fn resolve_values_from(
    client: &Client,
    scope_namespace: Option<&str>,
    source: &ValuesSource,
) -> Result<serde_json::Value, String> {
    let (kind, reference, is_secret) = match (&source.config_map_ref, &source.secret_ref) {
        (Some(reference), None) => ("ConfigMap", reference, false),
        (None, Some(reference)) => ("Secret", reference, true),
        (Some(_), Some(_)) => {
            return Err("configMapRef and secretRef are mutually exclusive".into());
        }
        (None, None) => return Err("one of configMapRef or secretRef is required".into()),
    };
    let Some(namespace) = reference.namespace.as_deref().or(scope_namespace) else {
        return Err(format!(
            "a cluster-scoped resource has no default namespace; set namespace ({kind} {})",
            reference.name
        ));
    };

    let document = read_referenced_key(
        client,
        kind,
        namespace,
        &reference.name,
        &reference.key,
        is_secret,
    )
    .await?;

    parse_values_document(&document).map_err(|e| {
        format!(
            "{kind} {namespace}/{} key {:?}: {e}",
            reference.name, reference.key
        )
    })
}

/// Read one key of a ConfigMap or Secret in `namespace`. Secret values arrive
/// base64-decoded and must be UTF-8. Errors are complete status messages.
async fn read_referenced_key(
    client: &Client,
    kind: &str,
    namespace: &str,
    name: &str,
    key: &str,
    is_secret: bool,
) -> Result<String, String> {
    if is_secret {
        let secret: Secret = Api::namespaced(client.clone(), namespace)
            .get(name)
            .await
            .map_err(|e| not_found_or(e, format!("{kind} {namespace}/{name}")))?;
        let bytes = secret
            .data
            .unwrap_or_default()
            .remove(key)
            .ok_or_else(|| format!("{kind} {namespace}/{name} has no key {key:?}"))?;
        String::from_utf8(bytes.0).map_err(|_| {
            format!("{kind} {namespace}/{name} key {key:?} is not UTF-8; the document must be text")
        })
    } else {
        let config_map: ConfigMap = Api::namespaced(client.clone(), namespace)
            .get(name)
            .await
            .map_err(|e| not_found_or(e, format!("{kind} {namespace}/{name}")))?;
        config_map
            .data
            .unwrap_or_default()
            .remove(key)
            .ok_or_else(|| format!("{kind} {namespace}/{name} has no key {key:?}"))
    }
}

/// Keep the common case in status short: a 404 is "not found", not a wall of
/// `ApiError` JSON.
fn not_found_or(error: kube::Error, reference: String) -> String {
    match error {
        kube::Error::Api(response) if response.code == 404 => format!("{reference} not found"),
        other => format!("{reference}: {other}"),
    }
}

/// The effective values for one component: golden defaults, then the external
/// document, then the CR's inline `values` (see [`crate::values`] for the full
/// order; `set` is handed to Helm as `--set` and wins on Helm's side).
async fn resolve_component_values(
    ctx: &Context,
    overrides: &OverridesContext<'_>,
    component: &str,
) -> Result<serde_json::Value, String> {
    let external = match overrides.overrides.values_from.get(component) {
        Some(source) => Some(
            resolve_values_from(&ctx.client, overrides.scope_namespace, source)
                .await
                .map_err(|reason| format!("valuesFrom {component}: {reason}"))?,
        ),
        None => None,
    };
    let inline = overrides.overrides.values.get(component);
    let values = effective_values(component, overrides.profile, external.as_ref(), inline);
    // The effective values are otherwise invisible until something is applied, which makes
    // "why did it plan that?" unanswerable from the cluster. Debug rather than info: this is
    // a diagnostic, not an event.
    tracing::debug!(
        component,
        profile = ?overrides.profile,
        values = %values,
        "effective values"
    );
    Ok(values)
}

/// The profile as it appears in a status message.
fn profile_name(profile: Profile) -> &'static str {
    match profile {
        Profile::Demo => "demo",
        Profile::Full => "full",
        Profile::Airgap => "airgap",
    }
}

/// Ready when every Deployment/StatefulSet of the release reports all replicas ready.
async fn workloads_ready(
    client: &Client,
    namespace: &str,
    release: &str,
) -> Result<(usize, usize), kube::Error> {
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let statefulsets: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);

    let mut deployments_listed = deployments
        .list(&ListParams::default().labels(&format!("app.kubernetes.io/instance={release}")))
        .await?;
    let mut statefulsets_listed = statefulsets
        .list(&ListParams::default().labels(&format!("app.kubernetes.io/instance={release}")))
        .await?;
    if deployments_listed.items.is_empty() && statefulsets_listed.items.is_empty() {
        // The common label is not universal: JupyterHub's chart stamps
        // `release=<name>` on its workloads instead.
        deployments_listed = deployments
            .list(&ListParams::default().labels(&format!("release={release}")))
            .await?;
        statefulsets_listed = statefulsets
            .list(&ListParams::default().labels(&format!("release={release}")))
            .await?;
    }

    let mut ready = 0;
    let mut total = 0;
    let deployment_counts = deployments_listed.items.iter().map(|d| {
        (
            d.spec.as_ref().and_then(|s| s.replicas),
            d.status.as_ref().and_then(|s| s.ready_replicas),
        )
    });
    let statefulset_counts = statefulsets_listed.items.iter().map(|s| {
        (
            s.spec.as_ref().and_then(|s| s.replicas),
            s.status.as_ref().and_then(|s| s.ready_replicas),
        )
    });
    for (desired, current) in deployment_counts.chain(statefulset_counts) {
        total += 1;
        ready += usize::from(workload_ready(desired, current));
    }
    Ok((ready, total))
}

/// A workload is ready when it reports at least its desired replicas.
fn workload_ready(desired: Option<i32>, ready: Option<i32>) -> bool {
    ready.unwrap_or(0) >= desired.unwrap_or(1)
}

/// Ready when every workload of the release is; a release with no workloads at
/// all is not ready (no evidence that anything runs).
fn ready_from_counts(ready: usize, total: usize) -> bool {
    total > 0 && ready == total
}

/// Apply (or plan) a component plan, returning a truthful per-component status.
async fn apply_plan(
    ctx: &Context,
    namespace: &str,
    plan: &[ComponentPlan],
    overrides: &OverridesContext<'_>,
    dry_run: bool,
    adopt: bool,
    previous: &[ComponentStatus],
) -> PlanOutcome {
    warn_unknown_overrides(plan, overrides.overrides);

    let mut outcome = PlanOutcome {
        components: Vec::with_capacity(plan.len()),
        values_resolved: true,
    };

    for component in plan {
        let Some(chart) = component.chart else {
            outcome.components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: false,
                message: Some("no adapter yet".into()),
                applied_hash: None,
            });
            continue;
        };

        let values = match resolve_component_values(ctx, overrides, component.name).await {
            Ok(values) => values,
            Err(message) => {
                tracing::warn!(component = component.name, %message, "values not resolved; component skipped");
                outcome.values_resolved = false;
                outcome.components.push(ComponentStatus {
                    name: component.name.to_string(),
                    ready: false,
                    message: Some(message),
                    applied_hash: None,
                });
                continue;
            }
        };

        let release = HelmRelease {
            name: component.name.to_string(),
            chart: chart.to_string(),
            repo: chart_repo(chart).map(str::to_string),
            version: chart_version(chart).map(str::to_string),
            namespace: namespace.to_string(),
            dry_run,
            set: overrides
                .overrides
                .set
                .get(component.name)
                .cloned()
                .unwrap_or_default(),
            values: Some(values),
        };
        let fingerprint = release.fingerprint();

        // Unchanged component: same args and values as the last successful apply,
        // previously ready, and Helm still reports the release deployed — no new
        // revision is needed. Only for a real apply; a dry run plans every time.
        let unchanged = !dry_run
            && previous.iter().any(|p| {
                p.name == component.name
                    && p.ready
                    && p.applied_hash.as_deref() == Some(fingerprint.as_str())
            });
        if unchanged
            && matches!(ctx.helm.run(release.status_args(), None).await, Ok(out) if is_deployed(&out))
        {
            outcome.components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: true,
                message: Some("unchanged".into()),
                applied_hash: Some(fingerprint),
            });
            continue;
        }

        // Adopt guard: never silently take over a release this operator did not
        // create. Releases it installed carry `OWNER_MARKER` in their history, so
        // they keep reconciling; `spec.adopt: true` is the opt-in for foreign ones.
        let foreign = !dry_run
            && !adopt
            && matches!(ctx.helm.run(release.history_args(), None).await, Ok(h) if !is_owned(&h));
        if foreign {
            outcome.components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: false,
                message: Some(
                    "release exists and is not owned by this operator — set spec.adopt: true to take it over"
                        .into(),
                ),
                applied_hash: None,
            });
            continue;
        }

        match ctx.helm.run(release.args(), release.values_yaml()).await {
            Ok(_) if dry_run => outcome.components.push(ComponentStatus {
                name: component.name.to_string(),
                ready: false,
                message: Some(format!(
                    "planned (dry-run, profile={})",
                    profile_name(overrides.profile)
                )),
                applied_hash: None,
            }),
            Ok(_) => {
                // The apply exited 0; the status tells the truth about the
                // workloads it left behind, not about Helm's exit code.
                let component_status =
                    match workloads_ready(&ctx.client, namespace, component.name).await {
                        Ok((ready, total)) => ComponentStatus {
                            name: component.name.to_string(),
                            ready: ready_from_counts(ready, total),
                            message: Some(format!("{ready}/{total} workloads ready")),
                            applied_hash: Some(fingerprint),
                        },
                        Err(e) => ComponentStatus {
                            name: component.name.to_string(),
                            ready: false,
                            message: Some(e.to_string()),
                            applied_hash: None,
                        },
                    };
                outcome.components.push(component_status);
            }
            Err(e) => {
                tracing::warn!(component = component.name, error = %e, "apply failed");
                outcome.components.push(ComponentStatus {
                    name: component.name.to_string(),
                    ready: false,
                    message: Some(e.to_string()),
                    applied_hash: None,
                });
            }
        }
    }

    outcome
}

fn phase(dry_run: bool, outcome: &PlanOutcome) -> &'static str {
    if !outcome.values_resolved {
        // An admin error (a missing or malformed values source) will not fix
        // itself, unlike a transient apply failure.
        "Degraded"
    } else if dry_run {
        "Planned"
    } else if outcome.components.iter().all(|c| c.ready) {
        "Ready"
    } else {
        "Progressing"
    }
}

/// Write one Lakehouse status. A merge patch: conditions and components are
/// replaced wholesale, the rest of the status is left alone.
async fn patch_lakehouse_status(
    ctx: &Context,
    lh: &Lakehouse,
    status: LakehouseStatus,
) -> Result<(), kube::Error> {
    let api: Api<Lakehouse> = match lh.namespace() {
        Some(ns) => Api::namespaced(ctx.client.clone(), &ns),
        None => Api::all(ctx.client.clone()),
    };
    api.patch_status(
        &lh.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    Ok(())
}

/// Finalizer that keeps a `Lakehouse` around until its releases are uninstalled
/// (wave 3a-ii, decision D18).
const LAKEHOUSE_FINALIZER: &str = "stardelt.io/lakehouse-deletion";

/// Label the SeaweedFS chart puts on its three data PVCs. Only volumes carrying
/// it are ever deleted; another release's volumes in the same namespace are
/// never touched.
const SEAWEEDFS_VOLUME_SELECTOR: &str = "app.kubernetes.io/instance=seaweedfs";

fn lakehouse_api(ctx: &Context, lh: &Lakehouse) -> Api<Lakehouse> {
    match lh.namespace() {
        Some(ns) => Api::namespaced(ctx.client.clone(), &ns),
        None => Api::all(ctx.client.clone()),
    }
}

/// Replace `metadata.finalizers` with exactly `finalizers` (a merge patch
/// replaces the whole list, so it never appends a duplicate).
async fn patch_lakehouse_finalizers(
    ctx: &Context,
    lh: &Lakehouse,
    finalizers: Vec<String>,
) -> Result<(), kube::Error> {
    lakehouse_api(ctx, lh)
        .patch(
            &lh.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
        )
        .await?;
    Ok(())
}

/// Add the deletion finalizer when it is missing; a resource that already
/// carries it is not patched again.
async fn ensure_lakehouse_finalizer(ctx: &Context, lh: &Lakehouse) -> Result<(), kube::Error> {
    if lh.finalizers().iter().any(|f| f == LAKEHOUSE_FINALIZER) {
        return Ok(());
    }
    let mut finalizers = lh.finalizers().to_vec();
    finalizers.push(LAKEHOUSE_FINALIZER.to_string());
    patch_lakehouse_finalizers(ctx, lh, finalizers).await
}

/// Remove the deletion finalizer so the API server can finish the deletion.
async fn remove_lakehouse_finalizer(ctx: &Context, lh: &Lakehouse) -> Result<(), kube::Error> {
    if !lh.finalizers().iter().any(|f| f == LAKEHOUSE_FINALIZER) {
        return Ok(());
    }
    let finalizers: Vec<String> = lh
        .finalizers()
        .iter()
        .filter(|f| *f != LAKEHOUSE_FINALIZER)
        .cloned()
        .collect();
    patch_lakehouse_finalizers(ctx, lh, finalizers).await
}

/// Delete a named object, treating `404` as success. A `Delete` is not atomic:
/// after a partial run the next reconcile starts from the top, and the objects
/// removed before the failure must not stop it.
async fn delete_ignoring_not_found<K>(api: &Api<K>, name: &str) -> Result<(), kube::Error>
where
    K: Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e),
    }
}

/// Remove the data of a `Lakehouse` (`deletionPolicy: Delete`, wave 3a-iii):
/// the SeaweedFS PVCs that Helm leaves behind on uninstall, then the catalog
/// CNPG `Cluster`, whose own PVC goes through its ownerReference and the
/// storage class's reclaim policy. `delete_object_store` is false for BYO S3:
/// that storage is not ours, so no volume is listed or deleted. The S3 Secrets
/// hold credentials, not data, and stay.
async fn delete_lakehouse_data(
    ctx: &Context,
    namespace: &str,
    delete_object_store: bool,
) -> Result<(), kube::Error> {
    if delete_object_store {
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), namespace);
        let params = ListParams {
            label_selector: Some(SEAWEEDFS_VOLUME_SELECTOR.to_string()),
            ..Default::default()
        };
        for pvc in pvcs.list(&params).await?.items {
            let name = pvc.name_any();
            delete_ignoring_not_found(&pvcs, &name).await?;
            tracing::info!(pvc = %name, %namespace, "object-store volume deleted");
        }
    }
    let clusters: Api<kube::api::DynamicObject> =
        Api::namespaced_with(ctx.client.clone(), namespace, &cnpg_cluster_resource());
    delete_ignoring_not_found(&clusters, CATALOG_CLUSTER).await?;
    tracing::info!(cluster = CATALOG_CLUSTER, %namespace, "catalog database deleted");
    Ok(())
}

/// Deletion path of a `Lakehouse` (waves 3a-ii and 3a-iii). The finalizer keeps
/// the resource visible until this returns, so it is called before any planning
/// or status.
///
/// `Retain` (the default) uninstalls the releases this operator owns in the
/// reverse of the apply order — query, catalog, storage — and leaves every
/// data-bearing object alone: the catalog CNPG `Cluster` (its deletion would take
/// its PVC with it), the S3 Secrets and the StatefulSet PVCs. The discovery
/// ConfigMap disappears through its ownerReference once the finalizer is removed.
/// `Delete` runs the same uninstalls and then [`delete_lakehouse_data`]; `404`s
/// are tolerated so a retry after a partial failure completes the rest.
async fn delete_lakehouse(lh: &Lakehouse, ctx: &Context) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let namespace = lh.namespace().unwrap_or_else(|| "default".into());

    if lh.spec.dry_run {
        // A dry-run resource never applied anything, so it owns no data to
        // delete, even with `deletionPolicy: Delete`.
        tracing::info!(
            lakehouse = %name,
            "dry-run Lakehouse deleted; nothing was applied to the cluster"
        );
        remove_lakehouse_finalizer(ctx, lh).await?;
        return Ok(Action::await_change());
    }

    for component in plan_lakehouse(&lh.spec).iter().rev() {
        let Some(chart) = component.chart else {
            // No adapter, so this operator never installed it.
            continue;
        };
        let release = HelmRelease {
            name: component.name.to_string(),
            chart: chart.to_string(),
            repo: chart_repo(chart).map(str::to_string),
            version: chart_version(chart).map(str::to_string),
            namespace: namespace.clone(),
            dry_run: false,
            set: Default::default(),
            values: None,
        };
        // Never uninstall a release this operator did not create. A history that
        // cannot be read is not treated as foreign: `helm uninstall
        // --ignore-not-found` deals with a release that does not exist.
        let foreign = !lh.spec.adopt
            && matches!(
                ctx.helm.run(release.history_args(), None).await,
                Ok(history) if !is_owned(&history)
            );
        if foreign {
            tracing::info!(
                lakehouse = %name,
                component = component.name,
                "release is not owned by this operator; leaving it alone"
            );
            continue;
        }
        if let Err(e) = ctx.helm.run(release.uninstall_args(), None).await {
            // Keep the finalizer; the next reconcile retries the whole reverse
            // order. `--ignore-not-found` makes the already-removed ones no-ops.
            tracing::warn!(
                lakehouse = %name,
                component = component.name,
                error = %e,
                "uninstall failed; keeping the finalizer"
            );
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
        tracing::info!(
            lakehouse = %name,
            component = component.name,
            "release uninstalled"
        );
    }

    if lh.spec.deletion_policy == DeletionPolicy::Delete {
        // The data goes only once every release is gone, so nothing is left
        // running against volumes that disappear. A failure keeps the finalizer
        // and the next reconcile retries from the top; 404s are success.
        let delete_object_store = lh.spec.storage.implementation == StorageImpl::Seaweedfs;
        if let Err(e) = delete_lakehouse_data(ctx, &namespace, delete_object_store).await {
            tracing::warn!(
                lakehouse = %name,
                error = %e,
                "data deletion failed; keeping the finalizer"
            );
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    }

    remove_lakehouse_finalizer(ctx, lh).await?;
    match lh.spec.deletion_policy {
        DeletionPolicy::Retain => tracing::info!(
            lakehouse = %name,
            "releases uninstalled; data retained (deletionPolicy Retain)"
        ),
        DeletionPolicy::Delete => tracing::info!(
            lakehouse = %name,
            "releases uninstalled and data deleted (deletionPolicy Delete)"
        ),
    }
    Ok(Action::await_change())
}

async fn reconcile_lakehouse(lh: Arc<Lakehouse>, ctx: Arc<Context>) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let namespace = lh.namespace().unwrap_or_else(|| "default".into());

    // Deletion first (wave 3a-ii): while the finalizer holds the object, its
    // releases are uninstalled in reverse apply order and no status is written.
    if lh.metadata.deletion_timestamp.is_some() {
        return delete_lakehouse(&lh, &ctx).await;
    }

    // v1alpha1 supports exactly one Lakehouse per namespace (decision D18):
    // release names are stable, so a second resource would Helm-upgrade the same
    // releases and apply the same Secret/Cluster names as the first. The oldest
    // resource (creationTimestamp, tie-break by name) owns the namespace; any
    // other one reports Blocked and plans nothing.
    let siblings = Api::<Lakehouse>::namespaced(ctx.client.clone(), &namespace)
        .list(&ListParams::default())
        .await?;
    let oldest = siblings
        .items
        .iter()
        .map(|item| (item.metadata.creation_timestamp.clone(), item.name_any()))
        .min();
    let blocked_by = match oldest {
        Some((_, oldest)) if oldest != name => Some(oldest),
        _ => None,
    };
    if let Some(peer) = blocked_by {
        let message =
            format!("one Lakehouse per namespace in v1alpha1: '{peer}' is older (decision D18)");
        let phase = "Blocked";
        let prev = lh
            .status
            .as_ref()
            .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"));
        let status = LakehouseStatus {
            phase: phase.into(),
            conditions: vec![ready_condition(
                phase,
                lh.metadata.generation,
                prev,
                &now_rfc3339(),
            )],
            observed_generation: lh.metadata.generation,
            components: vec![ComponentStatus {
                name: "lakehouse".into(),
                ready: false,
                message: Some(message),
                applied_hash: None,
            }],
        };
        patch_lakehouse_status(&ctx, &lh, status).await?;
        tracing::info!(lakehouse = %name, %namespace, older = %peer, "blocked: one Lakehouse per namespace in v1alpha1");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // From here the resource owns the namespace's releases, so it must not
    // disappear before they are uninstalled (wave 3a-ii). A Blocked resource
    // never applied anything and stays without the finalizer: deleting it must
    // not uninstall the releases of the resource that owns the namespace.
    ensure_lakehouse_finalizer(&ctx, &lh).await?;

    let plan = plan_lakehouse(&lh.spec);
    let overrides = OverridesContext {
        scope_namespace: Some(&namespace),
        overrides: &lh.spec.component_values,
        profile: lh.spec.profile,
    };
    let previous = lh
        .status
        .as_ref()
        .map(|s| s.components.as_slice())
        .unwrap_or(&[]);
    // Prerequisites first (decision D17): the S3 Secrets and the catalog database.
    // Lakekeeper is installed only once its database is Ready, so `helm --wait`
    // never blocks on a database that does not exist yet.
    let s3 = ensure_s3_secrets(&ctx, &lh, &namespace).await;
    let database = ensure_catalog_database(&ctx, &lh, &namespace).await;
    let database_ready = database.ready;
    let plan: Vec<ComponentPlan> = plan
        .into_iter()
        .filter(|c| c.name != "lakekeeper" || database_ready || lh.spec.dry_run)
        .collect();
    let mut outcome = apply_plan(
        &ctx,
        &namespace,
        &plan,
        &overrides,
        lh.spec.dry_run,
        lh.spec.adopt,
        previous,
    )
    .await;
    if !lh.spec.dry_run && !database_ready {
        outcome.components.push(ComponentStatus {
            name: "lakekeeper".into(),
            ready: false,
            message: Some("waiting for the catalog database".into()),
            applied_hash: None,
        });
    }
    // The bootstrap Job needs the charts it talks to.
    let ready = |name: &str| outcome.components.iter().any(|c| c.name == name && c.ready);
    let charts_ready = ready("lakekeeper")
        && (lh.spec.storage.implementation != StorageImpl::Seaweedfs || ready("seaweedfs"));
    let bootstrap = if charts_ready || lh.spec.dry_run {
        ensure_bootstrap(&ctx, &lh, &namespace, previous).await
    } else {
        ComponentStatus {
            name: "lakekeeper-bootstrap".into(),
            ready: false,
            message: Some("waiting for Lakekeeper and SeaweedFS".into()),
            applied_hash: None,
        }
    };
    // The discovery ConfigMap is published last: it advertises a working catalog.
    let discovery = ensure_discovery(&ctx, &lh, &namespace, bootstrap.ready).await;
    let mut components = vec![s3, database];
    components.append(&mut outcome.components);
    components.push(bootstrap);
    components.push(discovery);
    outcome.components = components;
    let phase = phase(lh.spec.dry_run, &outcome);
    // Check back quickly while the install is still converging.
    let requeue = if outcome.components.iter().all(|c| c.ready) {
        300
    } else {
        30
    };

    let prev = lh
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"));
    let status = LakehouseStatus {
        phase: phase.into(),
        conditions: vec![ready_condition(
            phase,
            lh.metadata.generation,
            prev,
            &now_rfc3339(),
        )],
        observed_generation: lh.metadata.generation,
        components: outcome.components,
    };
    patch_lakehouse_status(&ctx, &lh, status).await?;
    tracing::info!(lakehouse = %name, phase, dry_run = lh.spec.dry_run, "reconciled");
    Ok(Action::requeue(Duration::from_secs(requeue)))
}

async fn reconcile_platform(
    instance: Arc<PlatformInstance>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let name = instance.name_any();
    let plan = plan_platform(&instance.spec);
    let overrides = OverridesContext {
        // PlatformInstance is cluster-scoped: valuesFrom refs must name their
        // namespace explicitly.
        scope_namespace: None,
        overrides: &instance.spec.component_values,
        profile: instance.spec.profile,
    };
    let previous = instance
        .status
        .as_ref()
        .map(|s| s.components.as_slice())
        .unwrap_or(&[]);
    let mut outcome = apply_plan(
        &ctx,
        &instance.spec.namespace,
        &plan,
        &overrides,
        instance.spec.dry_run,
        false,
        previous,
    )
    .await;
    // The identity database comes before the contract that points the components at it.
    if let Some(database) = ensure_keycloak_pg(&ctx, &instance.spec).await {
        outcome.components.push(database);
    }
    // The credentials the realm import resolves come next: the workload below mounts them, and
    // the database is not needed for them.
    if let Some(secrets) = ensure_identity_secrets(&ctx, &instance.spec).await {
        outcome.components.push(secrets);
    }
    // The workload that serves the realm comes next: it reads the database and the
    // identity Secrets, and the contract below advertises its issuer.
    if let Some(keycloak) = ensure_keycloak(&ctx, &instance.spec).await {
        outcome.components.push(keycloak);
    }
    // The identity contract is published after the components it points the others to.
    if let Some(identity) = ensure_identity(&ctx, &instance.spec).await {
        outcome.components.push(identity);
    }
    let phase = phase(instance.spec.dry_run, &outcome);

    let prev = instance
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"));
    let status = PlatformInstanceStatus {
        phase: phase.into(),
        conditions: vec![ready_condition(
            phase,
            instance.metadata.generation,
            prev,
            &now_rfc3339(),
        )],
        observed_generation: instance.metadata.generation,
        components: outcome.components,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;
    use crate::helm::OWNER_MARKER;
    use crate::helm::testing::RecordingRunner;

    /// The tests describe one release, so the plan holds one component.
    fn plan() -> Vec<ComponentPlan> {
        vec![ComponentPlan {
            name: "trino",
            chart: Some("trino/trino"),
        }]
    }

    /// Overrides that change the release fingerprint when `workers` changes.
    fn overrides(workers: u8) -> ComponentValues {
        let mut values = ComponentValues::default();
        values.set.insert(
            "trino".into(),
            BTreeMap::from([("server.workers".to_string(), workers.to_string())]),
        );
        values
    }

    fn overrides_context(values: &ComponentValues) -> OverridesContext<'_> {
        OverridesContext {
            scope_namespace: Some("stardelt"),
            overrides: values,
            profile: Profile::Demo,
        }
    }

    /// A helm double that answers `helm history` like a release this operator owns.
    fn helm_stub() -> Arc<RecordingRunner> {
        let helm = Arc::new(RecordingRunner::default());
        helm.reply(
            "history",
            format!(r#"[{{"revision":1,"description":"{OWNER_MARKER}"}}]"#),
        );
        helm
    }

    fn upgrades(helm: &RecordingRunner) -> usize {
        helm.calls
            .lock()
            .expect("calls poisoned")
            .iter()
            .filter(|call| call.args.first().map(String::as_str) == Some("upgrade"))
            .count()
    }

    /// `DeploymentList` items with `ready` of `desired` replicas ready.
    fn deployment_list(ready: i32, desired: i32) -> String {
        format!(
            r#"[{{"metadata":{{"name":"trino-coordinator","namespace":"stardelt"}},"spec":{{"replicas":{desired}}},"status":{{"readyReplicas":{ready}}}}}]"#
        )
    }

    /// A kube client whose DeploymentList items come from `primary` or
    /// `fallback`, chosen by the label selector of the request; the
    /// StatefulSetList is always empty. Every request URL is recorded.
    fn mock_client(primary: &str, fallback: &str) -> (Client, Arc<Mutex<Vec<String>>>) {
        let requests: Arc<Mutex<Vec<String>>> = Arc::default();
        let recorder = requests.clone();
        let (primary, fallback) = (primary.to_string(), fallback.to_string());
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let path = request.uri().path().to_string();
            let query = request.uri().query().unwrap_or_default().to_string();
            recorder
                .lock()
                .expect("requests poisoned")
                .push(format!("{path}?{query}"));
            let (kind, items) = if path.ends_with("deployments") {
                let items = if query.contains("release%3D") {
                    fallback.clone()
                } else {
                    primary.clone()
                };
                ("DeploymentList", items)
            } else {
                ("StatefulSetList", "[]".to_string())
            };
            let document = format!(
                r#"{{"apiVersion":"apps/v1","kind":"{kind}","metadata":{{"resourceVersion":"1"}},"items":{items}}}"#
            );
            std::future::ready(Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(kube::client::Body::from(document.into_bytes()))
                    .expect("valid mock response"),
            ))
        });
        (Client::new(service, "stardelt"), requests)
    }

    #[test]
    fn ready_from_counts_matches_replica_truth() {
        assert!(!ready_from_counts(0, 0), "no workloads is not ready");
        assert!(!ready_from_counts(1, 2));
        assert!(ready_from_counts(2, 2));
    }

    #[tokio::test]
    async fn workloads_that_are_not_ready_do_not_report_ready() {
        let helm = helm_stub();
        // The primary (instance) selector finds nothing; the JupyterHub-style
        // `release=` fallback returns the workloads.
        let (client, requests) = mock_client("[]", &deployment_list(0, 2));
        let ctx = Context {
            client,
            helm: helm.clone(),
        };
        let values = ComponentValues::default();

        let outcome = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides_context(&values),
            false,
            false,
            &[],
        )
        .await;

        assert_eq!(upgrades(&helm), 1, "the release was applied");
        let status = &outcome.components[0];
        assert!(
            !status.ready,
            "0/1 workloads cannot report ready: {status:?}"
        );
        assert_eq!(status.message.as_deref(), Some("0/1 workloads ready"));
        assert_eq!(phase(false, &outcome), "Progressing");

        let requests = requests.lock().expect("requests poisoned");
        assert!(requests.iter().any(|r| r.contains("instance%3Dtrino")));
        assert!(
            requests.iter().any(|r| r.contains("release%3Dtrino")),
            "the fallback label must be tried: {requests:?}"
        );
    }

    #[tokio::test]
    async fn ready_workloads_report_ready() {
        let helm = helm_stub();
        let (client, _requests) = mock_client(&deployment_list(2, 2), "[]");
        let ctx = Context {
            client,
            helm: helm.clone(),
        };
        let values = ComponentValues::default();

        let outcome = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides_context(&values),
            false,
            false,
            &[],
        )
        .await;

        let status = &outcome.components[0];
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("1/1 workloads ready"));
        assert!(
            status.applied_hash.is_some(),
            "a successful apply records its fingerprint"
        );
        assert_eq!(phase(false, &outcome), "Ready");
    }

    #[tokio::test]
    async fn an_unchanged_component_is_not_reapplied() {
        let helm = helm_stub();
        helm.reply("status", r#"{"info":{"status":"deployed"}}"#);
        let (client, _requests) = mock_client(&deployment_list(2, 2), "[]");
        let ctx = Context {
            client,
            helm: helm.clone(),
        };
        let values = ComponentValues::default();
        let overrides = overrides_context(&values);

        let first = apply_plan(&ctx, "stardelt", &plan(), &overrides, false, false, &[]).await;
        assert!(first.components[0].ready, "{:?}", first.components[0]);
        assert_eq!(upgrades(&helm), 1);

        let second = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides,
            false,
            false,
            &first.components,
        )
        .await;

        assert_eq!(second.components[0].message.as_deref(), Some("unchanged"));
        assert!(second.components[0].ready);
        assert_eq!(
            second.components[0].applied_hash,
            first.components[0].applied_hash
        );
        assert_eq!(
            upgrades(&helm),
            1,
            "an unchanged component must not be re-upgraded"
        );
    }

    #[tokio::test]
    async fn changed_values_reapply() {
        let helm = helm_stub();
        let (client, _requests) = mock_client(&deployment_list(2, 2), "[]");
        let ctx = Context {
            client,
            helm: helm.clone(),
        };
        let before = overrides(1);
        let after = overrides(3);

        let first = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides_context(&before),
            false,
            false,
            &[],
        )
        .await;
        let second = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides_context(&after),
            false,
            false,
            &first.components,
        )
        .await;

        assert_ne!(
            first.components[0].applied_hash, second.components[0].applied_hash,
            "the fingerprint follows the values"
        );
        assert_eq!(upgrades(&helm), 2, "a changed component is re-applied");
    }

    #[tokio::test]
    async fn a_failed_release_reapplies_even_when_unchanged() {
        let helm = helm_stub();
        helm.reply("status", r#"{"info":{"status":"deployed"}}"#);
        let (client, _requests) = mock_client(&deployment_list(2, 2), "[]");
        let ctx = Context {
            client,
            helm: helm.clone(),
        };
        let values = ComponentValues::default();
        let overrides = overrides_context(&values);

        let first = apply_plan(&ctx, "stardelt", &plan(), &overrides, false, false, &[]).await;
        assert_eq!(upgrades(&helm), 1);

        helm.reply("status", r#"{"info":{"status":"failed"}}"#);
        let second = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides,
            false,
            false,
            &first.components,
        )
        .await;

        assert_eq!(
            second.components[0].applied_hash, first.components[0].applied_hash,
            "the fingerprint is unchanged…"
        );
        assert_eq!(
            upgrades(&helm),
            2,
            "…but a release that is not deployed is re-applied"
        );
    }

    #[tokio::test]
    async fn dry_runs_always_plan() {
        let helm = helm_stub();
        let (client, _requests) = mock_client("[]", "[]");
        let ctx = Context {
            client,
            helm: helm.clone(),
        };
        let values = ComponentValues::default();
        let previous = vec![ComponentStatus {
            name: "trino".into(),
            ready: true,
            message: None,
            applied_hash: Some("a-hash-from-an-earlier-pass".into()),
        }];

        let outcome = apply_plan(
            &ctx,
            "stardelt",
            &plan(),
            &overrides_context(&values),
            true,
            false,
            &previous,
        )
        .await;

        assert_eq!(
            outcome.components[0].message.as_deref(),
            Some("planned (dry-run, profile=demo)")
        );
        assert!(!outcome.components[0].ready);
        assert!(outcome.components[0].applied_hash.is_none());

        let calls = helm.calls.lock().expect("calls poisoned");
        assert!(
            calls.iter().any(
                |call| call.args.first().map(String::as_str) == Some("upgrade")
                    && call.args.contains(&"--dry-run".to_string())
            ),
            "a dry run renders the plan every time"
        );
        assert!(
            !calls
                .iter()
                .any(|call| call.args.first().map(String::as_str) == Some("status")),
            "a dry run does not consult the release"
        );
    }

    fn prev(status: &str, time: &str) -> Condition {
        Condition {
            type_: "Ready".into(),
            status: status.into(),
            reason: None,
            message: None,
            last_transition_time: Some(time.into()),
            observed_generation: None,
        }
    }

    #[test]
    fn ready_condition_true_only_for_ready() {
        assert_eq!(ready_condition("Ready", None, None, "t").status, "True");
        for phase in ["Planned", "Degraded", "Failed"] {
            assert_eq!(ready_condition(phase, None, None, "t").status, "False");
        }
    }

    #[test]
    fn ready_condition_keeps_time_when_unchanged() {
        let p = prev("True", "old");
        let c = ready_condition("Ready", Some(3), Some(&p), "new");
        assert_eq!(c.last_transition_time.as_deref(), Some("old"));
        assert_eq!(c.observed_generation, Some(3));
    }

    #[test]
    fn ready_condition_moves_time_on_flip() {
        let p = prev("True", "old");
        let c = ready_condition("Degraded", None, Some(&p), "new");
        assert_eq!(c.status, "False");
        assert_eq!(c.last_transition_time.as_deref(), Some("new"));
    }

    /// One request recorded by [`status_mock_client`] or [`deletion_mock_client`].
    #[derive(Debug)]
    struct RecordedRequest {
        method: String,
        path: String,
        query: String,
        body: String,
    }

    /// One `Lakehouse` for a `LakehouseList` answer, created at `created`.
    fn lakehouse_item(name: &str, created: &str) -> serde_json::Value {
        json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": {
                "name": name,
                "namespace": "stardelt",
                "creationTimestamp": created,
                "generation": 1
            },
            "spec": {}
        })
    }

    /// A kube client that answers the requests a reconciler makes before it
    /// patches a status: namespace reads return a Namespace, `GET …/lakehouses`
    /// returns a `LakehouseList` with `lakehouses` as its items, status patches
    /// echo a minimal document of the right kind. Every request is recorded, so
    /// a test can inspect the status the reconciler wrote.
    fn status_mock_client(
        lakehouses: serde_json::Value,
    ) -> (Client, Arc<Mutex<Vec<RecordedRequest>>>) {
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let lakehouses = lakehouses.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let query = request.uri().query().unwrap_or_default().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder
                    .lock()
                    .expect("requests poisoned")
                    .push(RecordedRequest {
                        method: method.clone(),
                        path: path.clone(),
                        query,
                        body: String::from_utf8_lossy(&body).to_string(),
                    });
                let document = if method == "GET" && path.contains("lakehouses") {
                    json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "LakehouseList",
                        "metadata": { "resourceVersion": "1" },
                        "items": lakehouses
                    })
                } else if method == "PATCH" && path.contains("lakehouses") {
                    json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "Lakehouse",
                        "metadata": { "name": "main", "namespace": "stardelt" },
                        "spec": {}
                    })
                } else if method == "PATCH" {
                    json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "PlatformInstance",
                        "metadata": { "name": "cluster" },
                        "spec": {}
                    })
                } else {
                    json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": { "name": "stardelt" }
                    })
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        (Client::new(service, "stardelt"), requests)
    }

    /// The `status` object of the last PATCH whose path contains `marker`.
    fn patched_status(
        requests: &Arc<Mutex<Vec<RecordedRequest>>>,
        marker: &str,
    ) -> serde_json::Value {
        let requests = requests.lock().expect("requests poisoned");
        let patch = requests
            .iter()
            .rev()
            .find(|r| r.method == "PATCH" && r.path.contains(marker))
            .unwrap_or_else(|| {
                let paths: Vec<&str> = requests.iter().map(|r| r.path.as_str()).collect();
                panic!("no status patch for {marker}: {paths:?}")
            });
        let body: serde_json::Value =
            serde_json::from_str(&patch.body).expect("patch body is JSON");
        body["status"].clone()
    }

    /// Every PATCH body recorded, oldest first.
    fn patch_bodies(requests: &Arc<Mutex<Vec<RecordedRequest>>>) -> Vec<serde_json::Value> {
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|r| r.method == "PATCH")
            .map(|r| serde_json::from_str(&r.body).expect("patch body is JSON"))
            .collect()
    }

    /// The `metadata.finalizers` lists of every finalizer patch, in order.
    fn patched_finalizers(requests: &Arc<Mutex<Vec<RecordedRequest>>>) -> Vec<Vec<String>> {
        patch_bodies(requests)
            .iter()
            .filter_map(|body| body["metadata"]["finalizers"].as_array())
            .map(|list| {
                list.iter()
                    .map(|item| item.as_str().expect("a finalizer is a string").to_string())
                    .collect()
            })
            .collect()
    }

    /// The release names of every `helm uninstall`, in call order.
    fn uninstalled(helm: &RecordingRunner) -> Vec<String> {
        helm.calls
            .lock()
            .expect("calls poisoned")
            .iter()
            .filter(|call| call.args.first().map(String::as_str) == Some("uninstall"))
            .map(|call| call.args[1].clone())
            .collect()
    }

    /// The paths of every `DELETE`, in call order.
    fn deleted(requests: &Arc<Mutex<Vec<RecordedRequest>>>) -> Vec<String> {
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|request| request.method == "DELETE")
            .map(|request| request.path.clone())
            .collect()
    }

    /// A kube client for the deletion tests. The PVC list returns exactly
    /// `pvcs`; deleting a name in `missing` answers `404` and one in `failed`
    /// answers `500`; every other delete succeeds. Requests are recorded in
    /// order, so a test can check that deletes happen before the finalizer
    /// patch.
    fn deletion_mock_client(
        pvcs: &[&str],
        missing: &[&str],
        failed: &[&str],
    ) -> (Client, Arc<Mutex<Vec<RecordedRequest>>>) {
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::default();
        let recorder = requests.clone();
        let pvcs: Vec<String> = pvcs.iter().map(|name| name.to_string()).collect();
        let missing: Vec<String> = missing.iter().map(|name| name.to_string()).collect();
        let failed: Vec<String> = failed.iter().map(|name| name.to_string()).collect();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let pvcs = pvcs.clone();
            let missing = missing.clone();
            let failed = failed.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let query = request.uri().query().unwrap_or_default().to_string();
            let name = path.rsplit('/').next().unwrap_or_default().to_string();
            let failure = if method == "DELETE" && failed.contains(&name) {
                Some((500, "InternalError"))
            } else if method == "DELETE" && missing.contains(&name) {
                Some((404, "NotFound"))
            } else {
                None
            };
            let document = if method == "GET" && path.ends_with("persistentvolumeclaims") {
                json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaimList",
                    "metadata": { "resourceVersion": "1" },
                    "items": pvcs.iter().map(|name| json!({
                        "apiVersion": "v1",
                        "kind": "PersistentVolumeClaim",
                        "metadata": { "name": name, "namespace": "stardelt" }
                    })).collect::<Vec<_>>()
                })
            } else if let Some((code, reason)) = failure {
                json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "metadata": {},
                    "status": "Failure",
                    "message": reason,
                    "reason": reason,
                    "code": code
                })
            } else if method == "DELETE" {
                json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "metadata": {},
                    "status": "Success"
                })
            } else if method == "PATCH" {
                json!({
                    "apiVersion": "stardelt.io/v1alpha1",
                    "kind": "Lakehouse",
                    "metadata": { "name": "main", "namespace": "stardelt" },
                    "spec": {}
                })
            } else {
                json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": { "name": "stardelt" }
                })
            };
            let status = failure.map_or(200, |(code, _)| code);
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                recorder
                    .lock()
                    .expect("requests poisoned")
                    .push(RecordedRequest {
                        method,
                        path,
                        query,
                        body: String::from_utf8_lossy(&body).to_string(),
                    });
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        (Client::new(service, "stardelt"), requests)
    }

    /// A `Lakehouse` the API server is deleting: it carries the operator's
    /// finalizer and no longer accepts spec changes.
    fn deleting_lakehouse(spec: serde_json::Value) -> Lakehouse {
        serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": {
                "name": "main",
                "namespace": "stardelt",
                "generation": 2,
                "deletionTimestamp": "2026-01-02T00:00:00Z",
                "finalizers": [LAKEHOUSE_FINALIZER]
            },
            "spec": spec
        }))
        .expect("deserialize Lakehouse")
    }

    /// A helm double whose releases carry the owner marker.
    fn owned_helm() -> Arc<RecordingRunner> {
        let helm = Arc::new(RecordingRunner::default());
        helm.reply(
            "history",
            format!(r#"[{{"revision":1,"description":"{OWNER_MARKER}"}}]"#),
        );
        helm
    }

    #[tokio::test]
    async fn lakehouse_status_carries_ready_and_observed_generation() {
        // The single-Lakehouse case: the only sibling is this one.
        let (client, requests) =
            status_mock_client(json!([lakehouse_item("main", "2026-01-01T00:00:00Z")]));
        let ctx = Arc::new(Context {
            client,
            helm: helm_stub(),
        });
        let lakehouse: Lakehouse = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt", "generation": 7 },
            "spec": {},
            "status": {
                "phase": "Planned",
                "observedGeneration": 6,
                "conditions": [{
                    "type": "Ready",
                    "status": "False",
                    "reason": "Planned",
                    "lastTransitionTime": "old"
                }]
            }
        }))
        .expect("deserialize Lakehouse");

        reconcile_lakehouse(Arc::new(lakehouse), ctx)
            .await
            .expect("reconcile Lakehouse");

        let status = patched_status(&requests, "lakehouses");
        assert_eq!(status["phase"], json!("Planned"));
        assert_eq!(status["observedGeneration"], json!(7));
        let ready = &status["conditions"][0];
        assert_eq!(ready["type"], json!("Ready"));
        assert_eq!(ready["status"], json!("False"));
        assert_eq!(ready["reason"], json!("Planned"));
        assert_eq!(ready["observedGeneration"], json!(7));
        assert_eq!(
            ready["lastTransitionTime"],
            json!("old"),
            "an unchanged Ready keeps its transition time"
        );
    }

    #[tokio::test]
    async fn lakehouse_status_ends_with_the_discovery_component() {
        let (client, requests) =
            status_mock_client(json!([lakehouse_item("main", "2026-01-01T00:00:00Z")]));
        let ctx = Arc::new(Context {
            client,
            helm: helm_stub(),
        });
        let lakehouse: Lakehouse = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt", "generation": 1 },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");

        reconcile_lakehouse(Arc::new(lakehouse), ctx)
            .await
            .expect("reconcile Lakehouse");

        let status = patched_status(&requests, "lakehouses");
        let components = status["components"].as_array().expect("components");
        let last = components.last().expect("at least one component");
        assert_eq!(last["name"], json!("discovery"));
        assert_eq!(last["ready"], json!(false));
        assert_eq!(last["message"], json!("planned (dry-run)"));
        assert_eq!(
            components[components.len() - 2]["name"],
            json!("lakekeeper-bootstrap"),
            "discovery is published after the bootstrap"
        );
    }

    #[tokio::test]
    async fn a_second_lakehouse_is_blocked_and_applies_nothing() {
        // `main` is older than `scratch-two`, so reconciling the newer one must
        // report Blocked instead of planning.
        let (client, requests) = status_mock_client(json!([
            lakehouse_item("main", "2026-01-01T00:00:00Z"),
            lakehouse_item("scratch-two", "2026-01-02T00:00:00Z"),
        ]));
        let helm = helm_stub();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });
        let lakehouse: Lakehouse = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "scratch-two", "namespace": "stardelt", "generation": 3 },
            "spec": { "profile": "demo", "dryRun": true }
        }))
        .expect("deserialize Lakehouse");

        let action = reconcile_lakehouse(Arc::new(lakehouse), ctx)
            .await
            .expect("reconcile Lakehouse");

        assert_eq!(
            action,
            Action::requeue(Duration::from_secs(30)),
            "a blocked Lakehouse checks back in case the older peer disappears"
        );
        let status = patched_status(&requests, "lakehouses");
        assert_eq!(status["phase"], json!("Blocked"));
        assert_eq!(status["observedGeneration"], json!(3));
        let ready = &status["conditions"][0];
        assert_eq!(ready["type"], json!("Ready"));
        assert_eq!(ready["status"], json!("False"));
        assert_eq!(ready["reason"], json!("Blocked"));
        let components = status["components"].as_array().expect("components");
        assert_eq!(components.len(), 1, "only the blocking reason is reported");
        assert_eq!(components[0]["name"], json!("lakehouse"));
        assert_eq!(components[0]["ready"], json!(false));
        assert_eq!(
            components[0]["message"],
            json!("one Lakehouse per namespace in v1alpha1: 'main' is older (decision D18)")
        );
        assert!(
            helm.calls.lock().expect("calls poisoned").is_empty(),
            "a blocked Lakehouse must not invoke helm"
        );
    }

    #[tokio::test]
    async fn the_oldest_lakehouse_still_plans() {
        let (client, requests) = status_mock_client(json!([
            lakehouse_item("main", "2026-01-01T00:00:00Z"),
            lakehouse_item("scratch-two", "2026-01-02T00:00:00Z"),
        ]));
        let helm = helm_stub();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });
        let lakehouse: Lakehouse = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt", "generation": 2 },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");

        reconcile_lakehouse(Arc::new(lakehouse), ctx)
            .await
            .expect("reconcile Lakehouse");

        let status = patched_status(&requests, "lakehouses");
        assert_eq!(status["phase"], json!("Planned"));
        assert!(
            !helm.calls.lock().expect("calls poisoned").is_empty(),
            "the oldest Lakehouse still plans its releases"
        );
    }

    #[tokio::test]
    async fn a_missing_finalizer_is_added_once_before_planning() {
        let (client, requests) =
            status_mock_client(json!([lakehouse_item("main", "2026-01-01T00:00:00Z")]));
        let ctx = Arc::new(Context {
            client,
            helm: helm_stub(),
        });
        let lakehouse: Lakehouse = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "stardelt", "generation": 1 },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");

        reconcile_lakehouse(Arc::new(lakehouse), ctx)
            .await
            .expect("reconcile Lakehouse");

        assert_eq!(
            patched_finalizers(&requests),
            vec![vec![LAKEHOUSE_FINALIZER.to_string()]],
            "the finalizer is added exactly once"
        );
    }

    #[tokio::test]
    async fn a_present_finalizer_is_not_patched_again() {
        let (client, requests) =
            status_mock_client(json!([lakehouse_item("main", "2026-01-01T00:00:00Z")]));
        let ctx = Arc::new(Context {
            client,
            helm: helm_stub(),
        });
        let lakehouse: Lakehouse = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": {
                "name": "main",
                "namespace": "stardelt",
                "generation": 1,
                "finalizers": [LAKEHOUSE_FINALIZER]
            },
            "spec": {}
        }))
        .expect("deserialize Lakehouse");

        reconcile_lakehouse(Arc::new(lakehouse), ctx)
            .await
            .expect("reconcile Lakehouse");

        assert!(
            patched_finalizers(&requests).is_empty(),
            "a Lakehouse that already carries the finalizer is not patched again"
        );
    }

    #[tokio::test]
    async fn retain_deletion_uninstalls_owned_releases_in_reverse_order() {
        let (client, requests) = status_mock_client(json!([]));
        let helm = owned_helm();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(json!({ "dryRun": false }))),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(action, Action::await_change());
        assert_eq!(
            uninstalled(&helm),
            vec![
                "trino".to_string(),
                "lakekeeper".to_string(),
                "seaweedfs".to_string()
            ],
            "deletion runs the plan in reverse: query, catalog, storage"
        );
        assert_eq!(
            patched_finalizers(&requests),
            vec![Vec::<String>::new()],
            "the finalizer is removed after the last release"
        );

        let requests = requests.lock().expect("requests poisoned");
        assert!(
            !requests.iter().any(|r| r.method == "DELETE"),
            "Retain deletes nothing: {requests:?}"
        );
        assert!(
            !requests
                .iter()
                .any(|r| r.path.contains("clusters") || r.path.contains("persistentvolumeclaims")),
            "the catalog Cluster and the PVCs stay: {requests:?}"
        );
    }

    #[tokio::test]
    async fn dry_run_deletion_makes_no_helm_calls_and_removes_the_finalizer() {
        let (client, requests) = status_mock_client(json!([]));
        let helm = owned_helm();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        // The default dryRun is true: nothing was ever applied to the cluster.
        let action = reconcile_lakehouse(Arc::new(deleting_lakehouse(json!({}))), ctx)
            .await
            .expect("delete Lakehouse");

        assert_eq!(action, Action::await_change());
        assert!(
            helm.calls.lock().expect("calls poisoned").is_empty(),
            "a dry-run resource never touched the cluster"
        );
        assert_eq!(
            patched_finalizers(&requests),
            vec![Vec::<String>::new()],
            "the finalizer is removed without uninstalling anything"
        );
    }

    #[tokio::test]
    async fn a_foreign_release_is_never_uninstalled() {
        let (client, requests) = status_mock_client(json!([]));
        let helm = Arc::new(RecordingRunner::default());
        helm.reply(
            "history",
            r#"[{"revision":1,"description":"Install complete"}]"#,
        );
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(json!({ "dryRun": false }))),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(action, Action::await_change());
        assert!(
            uninstalled(&helm).is_empty(),
            "a release without the owner marker is left alone"
        );
        assert_eq!(
            patched_finalizers(&requests),
            vec![Vec::<String>::new()],
            "the finalizer is still removed"
        );
    }

    #[tokio::test]
    async fn an_uninstall_error_keeps_the_finalizer() {
        let (client, requests) = status_mock_client(json!([]));
        let helm = owned_helm();
        helm.fail("uninstall", "helm uninstall failed: boom");
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(json!({ "dryRun": false }))),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(
            action,
            Action::requeue(Duration::from_secs(30)),
            "a failed uninstall is retried"
        );
        assert_eq!(
            uninstalled(&helm),
            vec!["trino".to_string()],
            "the reverse order stops at the first failure"
        );
        assert!(
            patched_finalizers(&requests).is_empty(),
            "the finalizer stays until every release is gone"
        );
    }

    #[tokio::test]
    async fn delete_deletion_uninstalls_then_removes_volumes_and_catalog() {
        let (client, requests) = deletion_mock_client(
            &[
                "data-seaweedfs-volume-0",
                "data-filer-seaweedfs-filer-0",
                "data-stardelt-seaweedfs-master-0",
            ],
            &[],
            &[],
        );
        let helm = owned_helm();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(
                json!({ "dryRun": false, "deletionPolicy": "Delete" }),
            )),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(action, Action::await_change());
        assert_eq!(
            uninstalled(&helm),
            vec![
                "trino".to_string(),
                "lakekeeper".to_string(),
                "seaweedfs".to_string()
            ],
            "the releases go first, in reverse plan order"
        );
        assert_eq!(
            deleted(&requests),
            vec![
                "/api/v1/namespaces/stardelt/persistentvolumeclaims/data-seaweedfs-volume-0"
                    .to_string(),
                "/api/v1/namespaces/stardelt/persistentvolumeclaims/data-filer-seaweedfs-filer-0"
                    .to_string(),
                "/api/v1/namespaces/stardelt/persistentvolumeclaims/data-stardelt-seaweedfs-master-0"
                    .to_string(),
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/lakekeeper-pg"
                    .to_string(),
            ],
            "the object-store volumes go before the catalog Cluster"
        );
        assert_eq!(
            patched_finalizers(&requests),
            vec![Vec::<String>::new()],
            "the finalizer is removed only after every delete"
        );

        let requests = requests.lock().expect("requests poisoned");
        assert!(
            requests.iter().any(|request| request.method == "GET"
                && request.path.ends_with("persistentvolumeclaims")
                && request
                    .query
                    .contains("labelSelector=app.kubernetes.io%2Finstance%3Dseaweedfs")),
            "only the SeaweedFS volumes are listed: {requests:?}"
        );
        let last_delete = requests
            .iter()
            .rposition(|request| request.method == "DELETE")
            .expect("the data deletes were issued");
        let finalizer_patch = requests
            .iter()
            .rposition(|request| {
                request.method == "PATCH" && request.body.contains("\"finalizers\"")
            })
            .expect("the finalizer was patched");
        assert!(
            finalizer_patch > last_delete,
            "the finalizer is removed after the deletes: {requests:?}"
        );
    }

    #[tokio::test]
    async fn byo_s3_delete_skips_the_volumes_but_deletes_the_catalog() {
        let (client, requests) = deletion_mock_client(&[], &[], &[]);
        let helm = owned_helm();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(json!({
                "dryRun": false,
                "deletionPolicy": "Delete",
                "storage": { "implementation": "byoS3", "bucket": "s3://acme-lake" }
            }))),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(action, Action::await_change());
        assert_eq!(
            deleted(&requests),
            vec![
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/lakekeeper-pg"
                    .to_string()
            ],
            "BYO S3 deletes no volume; the catalog is still ours"
        );
        let requests = requests.lock().expect("requests poisoned");
        assert!(
            !requests
                .iter()
                .any(|request| request.path.contains("persistentvolumeclaims")),
            "the object store is not ours, so its PVCs are not even listed: {requests:?}"
        );
    }

    #[tokio::test]
    async fn a_volume_delete_failure_keeps_the_finalizer() {
        let (client, requests) = deletion_mock_client(
            &[
                "data-seaweedfs-volume-0",
                "data-filer-seaweedfs-filer-0",
                "data-stardelt-seaweedfs-master-0",
            ],
            &[],
            &["data-seaweedfs-volume-0"],
        );
        let helm = owned_helm();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(
                json!({ "dryRun": false, "deletionPolicy": "Delete" }),
            )),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(
            action,
            Action::requeue(Duration::from_secs(30)),
            "a failed delete is retried"
        );
        assert_eq!(
            deleted(&requests),
            vec![
                "/api/v1/namespaces/stardelt/persistentvolumeclaims/data-seaweedfs-volume-0"
                    .to_string()
            ],
            "the volumes stop at the first failure"
        );
        assert!(
            patched_finalizers(&requests).is_empty(),
            "the finalizer stays until every data object is gone"
        );
    }

    #[tokio::test]
    async fn a_delete_retry_tolerates_404s_and_finishes() {
        // A previous attempt removed two volumes and the Cluster but failed on
        // the last one; the next reconcile sees a stale list and 404s, which
        // are success.
        let (client, requests) = deletion_mock_client(
            &["data-stardelt-seaweedfs-master-0"],
            &["data-stardelt-seaweedfs-master-0", "lakekeeper-pg"],
            &[],
        );
        let helm = owned_helm();
        let ctx = Arc::new(Context {
            client,
            helm: helm.clone(),
        });

        let action = reconcile_lakehouse(
            Arc::new(deleting_lakehouse(
                json!({ "dryRun": false, "deletionPolicy": "Delete" }),
            )),
            ctx,
        )
        .await
        .expect("delete Lakehouse");

        assert_eq!(action, Action::await_change());
        assert_eq!(
            deleted(&requests),
            vec![
                "/api/v1/namespaces/stardelt/persistentvolumeclaims/data-stardelt-seaweedfs-master-0"
                    .to_string(),
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/lakekeeper-pg"
                    .to_string(),
            ],
            "the retry asks for the remaining objects and tolerates missing ones"
        );
        assert_eq!(
            patched_finalizers(&requests),
            vec![Vec::<String>::new()],
            "a retry that finds everything already gone removes the finalizer"
        );
    }

    #[tokio::test]
    async fn platform_status_carries_ready_and_observed_generation() {
        let (client, requests) = status_mock_client(json!([]));
        let ctx = Arc::new(Context {
            client,
            helm: helm_stub(),
        });
        let instance: PlatformInstance = serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster", "generation": 7 },
            "spec": {},
            "status": {
                "phase": "Planned",
                "observedGeneration": 6,
                "conditions": [{
                    "type": "Ready",
                    "status": "False",
                    "reason": "Planned",
                    "lastTransitionTime": "old"
                }]
            }
        }))
        .expect("deserialize PlatformInstance");

        reconcile_platform(Arc::new(instance), ctx)
            .await
            .expect("reconcile PlatformInstance");

        let status = patched_status(&requests, "platforminstances");
        assert_eq!(status["phase"], json!("Planned"));
        assert_eq!(status["observedGeneration"], json!(7));
        let ready = &status["conditions"][0];
        assert_eq!(ready["type"], json!("Ready"));
        assert_eq!(ready["status"], json!("False"));
        assert_eq!(ready["reason"], json!("Planned"));
        assert_eq!(ready["observedGeneration"], json!(7));
        assert_eq!(
            ready["lastTransitionTime"],
            json!("old"),
            "an unchanged Ready keeps its transition time"
        );
    }
}
