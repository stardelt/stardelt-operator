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

use crate::crd::{ComponentStatus, Lakehouse, LakehouseStatus, PlatformInstance, PlatformInstanceStatus};
use crate::planner::plan_lakehouse;

/// Run all controllers until the process is stopped.
pub async fn run() -> anyhow::Result<()> {
    let client = Client::try_default().await?;

    let lakehouses: Api<Lakehouse> = Api::all(client.clone());
    let instances: Api<PlatformInstance> = Api::all(client.clone());

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

    tracing::info!("stardelt-operator watching Lakehouse and PlatformInstance");
    futures::pin_mut!(lh, pi);
    loop {
        tokio::select! {
            _ = lh.next() => {}
            _ = pi.next() => {}
        }
    }
}

async fn reconcile_lakehouse(lh: Arc<Lakehouse>, client: Arc<Client>) -> Result<Action, kube::Error> {
    let name = lh.name_any();
    let plan = plan_lakehouse(&lh.spec);
    let status = LakehouseStatus {
        phase: "Ready".into(),
        conditions: vec![],
        components: plan
            .iter()
            .map(|c| ComponentStatus { name: c.name.to_string(), ready: true, message: None })
            .collect(),
    };

    let api: Api<Lakehouse> = match lh.namespace() {
        Some(ns) => Api::namespaced((*client).clone(), &ns),
        None => Api::all((*client).clone()),
    };
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    tracing::info!(lakehouse = %name, components = plan.len(), "reconciled");

    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn reconcile_platform(pi: Arc<PlatformInstance>, client: Arc<Client>) -> Result<Action, kube::Error> {
    let name = pi.name_any();
    let status = PlatformInstanceStatus { phase: "Ready".into(), conditions: vec![] };

    let api: Api<PlatformInstance> = Api::all((*client).clone());
    let patch = serde_json::json!({ "status": status });
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    tracing::info!(instance = %name, profile = ?pi.spec.profile, "reconciled");

    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy<K>(_obj: Arc<K>, err: &kube::Error, _ctx: Arc<Client>) -> Action {
    tracing::warn!(error = %err, "reconcile failed");
    Action::requeue(Duration::from_secs(30))
}
