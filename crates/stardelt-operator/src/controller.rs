//! Controller runtime.
//!
//! Watches the stardelt CRDs and reconciles them into real resources:
//! - `PlatformInstance` → platform-level prerequisites (CNPG operator), once
//! - `Lakehouse` → its namespace-scoped plan (storage + catalog)
//!
//! Applies go through Helm. Both resources default to `dryRun: true`, so a new
//! resource plans first and changes nothing until explicitly flipped.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{ConfigMap, Secret},
};
use kube::{
    Api, Client, ResourceExt,
    api::{ListParams, Patch, PatchParams},
    runtime::{Controller, controller::Action, watcher},
};
use serde_json::json;

use crate::crd::Profile;
use crate::crd::{
    ComponentStatus, ComponentValues, Condition, Lakehouse, LakehouseStatus, PlatformInstance,
    PlatformInstanceStatus, StorageImpl, ValuesSource,
};
use crate::helm::{CommandHelmRunner, HelmRelease, HelmRunner, is_deployed, is_owned};
use crate::planner::{ComponentPlan, chart_repo, chart_version, plan_lakehouse, plan_platform};
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

    tracing::info!("stardelt-operator watching PlatformInstance and Lakehouse");
    futures::pin_mut!(lakehouses, platforms);
    loop {
        tokio::select! {
            _ = lakehouses.next() => {}
            _ = platforms.next() => {}
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

async fn reconcile_lakehouse(lh: Arc<Lakehouse>, ctx: Arc<Context>) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let namespace = lh.namespace().unwrap_or_else(|| "default".into());
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

    /// One request recorded by [`status_mock_client`].
    struct RecordedRequest {
        method: String,
        path: String,
        body: String,
    }

    /// A kube client that answers the requests a reconciler makes before it
    /// patches a status: namespace reads return a Namespace, status patches echo
    /// a minimal document of the right kind. Every request is recorded, so a
    /// test can inspect the status the reconciler wrote.
    fn status_mock_client() -> (Client, Arc<Mutex<Vec<RecordedRequest>>>) {
        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
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
                        body: String::from_utf8_lossy(&body).to_string(),
                    });
                let document = if method == "PATCH" && path.contains("lakehouses") {
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

    #[tokio::test]
    async fn lakehouse_status_carries_ready_and_observed_generation() {
        let (client, requests) = status_mock_client();
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
        let (client, requests) = status_mock_client();
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
    async fn platform_status_carries_ready_and_observed_generation() {
        let (client, requests) = status_mock_client();
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
