//! Leader election (stardelt-operator#75): one replica reconciles, the others wait.
//!
//! The `coordination.k8s.io/v1` Lease `stardelt-operator-leader` in the operator's own
//! namespace names the replica that may reconcile. A replica
//!
//! 1. waits in `acquire` until it holds the Lease: there is none yet, it names nobody,
//!    it already names this pod, or its holder stopped renewing it;
//! 2. then starts the controllers;
//! 3. renews the Lease in `keep` every `RENEW_EVERY`. When it cannot renew for
//!    `RENEW_DEADLINE` (shorter than the lease), or another pod holds it, `keep` returns
//!    and the process exits, so the pod restarts as a follower and never runs a
//!    `helm upgrade` next to a new leader.
//!
//! "Stopped renewing" is measured on this replica's own clock: the Lease's
//! `resourceVersion` has not changed for `LEASE_SECONDS`. The holder's `renewTime` is
//! never compared with this clock, so clock skew between nodes cannot hand over early.
//!
//! Every write carries the `resourceVersion` it read: two replicas that race for a free
//! Lease cannot both win, one of them gets 409 Conflict and reads again.
//!
//! Without `POD_NAME` and `POD_NAMESPACE` (a local `cargo run`) there is no election.

use std::time::{Duration, Instant};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::Client;
use kube::api::{Api, PostParams};

/// The Lease's name in the operator's namespace.
pub const LEASE_NAME: &str = "stardelt-operator-leader";
/// How long a Lease that is not renewed keeps its holder.
pub const LEASE_SECONDS: i32 = 15;
/// How often the leader renews.
pub const RENEW_EVERY: Duration = Duration::from_secs(5);
/// How long one renewal may take before it counts as failed.
pub const RENEW_TIMEOUT: Duration = Duration::from_secs(2);
/// The leader stops after this long without a successful renewal: well inside
/// `LEASE_SECONDS`, so it has stopped before a follower takes the Lease.
pub const RENEW_DEADLINE: Duration = Duration::from_secs(10);
/// How often a follower reads the Lease again.
pub const RETRY_EVERY: Duration = Duration::from_secs(2);

/// What a replica does with the Lease it just read. A pure function, unit-tested.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    /// No Lease exists: create it, naming this pod.
    Create,
    /// The Lease names nobody, or its holder stopped renewing: write this pod into it.
    Take,
    /// The Lease already names this pod: renew it.
    Renew,
    /// Another pod holds a live Lease: read again later.
    Wait,
}

/// `holder` is the Lease's `holderIdentity` (`None` when there is no Lease), `unchanged_for`
/// how long this replica has seen the same `resourceVersion`.
pub fn decide(
    lease_exists: bool,
    holder: Option<&str>,
    me: &str,
    unchanged_for: Duration,
) -> Decision {
    if !lease_exists {
        return Decision::Create;
    }
    match holder.filter(|h| !h.is_empty()) {
        None => Decision::Take,
        Some(h) if h == me => Decision::Renew,
        Some(_) if unchanged_for >= Duration::from_secs(LEASE_SECONDS as u64) => Decision::Take,
        Some(_) => Decision::Wait,
    }
}

/// This pod's name and namespace from the downward API, or `None` outside a pod.
pub fn identity() -> Option<(String, String)> {
    let name = std::env::var("POD_NAME").ok().filter(|v| !v.is_empty())?;
    let namespace = std::env::var("POD_NAMESPACE")
        .ok()
        .filter(|v| !v.is_empty())?;
    Some((name, namespace))
}

/// The Lease as this pod writes it. `previous` keeps the read `resourceVersion` (the
/// optimistic-concurrency guard) and the original `acquireTime` on a renewal.
fn lease_for(me: &str, previous: Option<&Lease>, renewing: bool) -> Lease {
    let now = MicroTime(k8s_openapi::jiff::Timestamp::now());
    let previous_spec = previous.and_then(|l| l.spec.as_ref());
    let acquire_time = match previous_spec {
        Some(spec) if renewing => spec.acquire_time.clone().or(Some(now.clone())),
        _ => Some(now.clone()),
    };
    let transitions = previous_spec.and_then(|s| s.lease_transitions).unwrap_or(0);
    Lease {
        metadata: ObjectMeta {
            name: Some(LEASE_NAME.into()),
            resource_version: previous.and_then(|l| l.metadata.resource_version.clone()),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(me.into()),
            lease_duration_seconds: Some(LEASE_SECONDS),
            acquire_time,
            renew_time: Some(now),
            lease_transitions: Some(if renewing {
                transitions
            } else {
                transitions + 1
            }),
            ..LeaseSpec::default()
        }),
    }
}

fn is_conflict(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(r) if r.code == 409)
}

/// Block until this pod holds the Lease.
pub async fn acquire(client: Client, namespace: &str, me: &str) -> anyhow::Result<()> {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    let mut seen: Option<(String, Instant)> = None;
    let mut waiting_logged = false;
    loop {
        let lease = api.get_opt(LEASE_NAME).await?;
        let version = lease
            .as_ref()
            .and_then(|l| l.metadata.resource_version.clone())
            .unwrap_or_default();
        let unchanged_for = match &seen {
            Some((v, at)) if *v == version => at.elapsed(),
            _ => {
                seen = Some((version, Instant::now()));
                Duration::ZERO
            }
        };
        let holder = lease
            .as_ref()
            .and_then(|l| l.spec.as_ref())
            .and_then(|s| s.holder_identity.clone());
        let written = match decide(lease.is_some(), holder.as_deref(), me, unchanged_for) {
            Decision::Create => {
                api.create(&PostParams::default(), &lease_for(me, None, false))
                    .await
            }
            Decision::Take => {
                api.replace(
                    LEASE_NAME,
                    &PostParams::default(),
                    &lease_for(me, lease.as_ref(), false),
                )
                .await
            }
            Decision::Renew => {
                api.replace(
                    LEASE_NAME,
                    &PostParams::default(),
                    &lease_for(me, lease.as_ref(), true),
                )
                .await
            }
            Decision::Wait => {
                if !waiting_logged {
                    tracing::info!(
                        holder = holder.as_deref().unwrap_or(""),
                        "another replica leads; waiting for lease {LEASE_NAME}"
                    );
                    waiting_logged = true;
                }
                tokio::time::sleep(RETRY_EVERY).await;
                continue;
            }
        };
        match written {
            Ok(_) => {
                tracing::info!(pod = me, "acquired lease {LEASE_NAME}; this replica leads");
                return Ok(());
            }
            Err(e) if is_conflict(&e) => {}
            Err(e) => return Err(e.into()),
        }
        tokio::time::sleep(RETRY_EVERY).await;
    }
}

/// The result of one renewal attempt.
enum Renewal {
    Renewed,
    /// Another pod holds the Lease: stop at once.
    Lost(String),
    /// The API server refused or did not answer: try again until `RENEW_DEADLINE`.
    Failed(String),
}

async fn renew_once(api: &Api<Lease>, me: &str) -> Renewal {
    match api.get_opt(LEASE_NAME).await {
        Ok(Some(lease)) => {
            let holder = lease
                .spec
                .as_ref()
                .and_then(|s| s.holder_identity.clone())
                .unwrap_or_default();
            if holder != me {
                return Renewal::Lost(format!("lease {LEASE_NAME} is held by {holder:?}"));
            }
            let renewed = lease_for(me, Some(&lease), true);
            match api
                .replace(LEASE_NAME, &PostParams::default(), &renewed)
                .await
            {
                Ok(_) => Renewal::Renewed,
                Err(e) => Renewal::Failed(e.to_string()),
            }
        }
        Ok(None) => Renewal::Failed(format!("lease {LEASE_NAME} was deleted")),
        Err(e) => Renewal::Failed(e.to_string()),
    }
}

/// Renew the Lease until leadership is lost, and return the reason. The caller exits.
///
/// The leader gives up after `RENEW_DEADLINE` without a successful renewal. That is
/// shorter than `LEASE_SECONDS`, so it has stopped before a follower may take over.
pub async fn keep(client: Client, namespace: String, me: String) -> String {
    let api: Api<Lease> = Api::namespaced(client, &namespace);
    let mut last_renewal = Instant::now();
    loop {
        tokio::time::sleep(RENEW_EVERY).await;
        let outcome = tokio::time::timeout(RENEW_TIMEOUT, renew_once(&api, &me))
            .await
            .unwrap_or_else(|_| Renewal::Failed("the API server did not answer in time".into()));
        match outcome {
            Renewal::Renewed => last_renewal = Instant::now(),
            Renewal::Lost(reason) => return reason,
            Renewal::Failed(reason) => {
                tracing::warn!(reason = %reason, "could not renew lease {LEASE_NAME}");
                if last_renewal.elapsed() >= RENEW_DEADLINE {
                    return format!(
                        "lease {LEASE_NAME} not renewed for {}s: {reason}",
                        RENEW_DEADLINE.as_secs()
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ME: &str = "stardelt-operator-abc";
    const OTHER: &str = "stardelt-operator-xyz";

    #[test]
    fn no_lease_is_created() {
        assert_eq!(decide(false, None, ME, Duration::ZERO), Decision::Create);
    }

    #[test]
    fn a_lease_that_names_nobody_is_taken() {
        assert_eq!(decide(true, None, ME, Duration::ZERO), Decision::Take);
        assert_eq!(decide(true, Some(""), ME, Duration::ZERO), Decision::Take);
    }

    #[test]
    fn our_own_lease_is_renewed_at_once() {
        // A restarted container in the same pod has the same name and leads again.
        assert_eq!(decide(true, Some(ME), ME, Duration::ZERO), Decision::Renew);
    }

    #[test]
    fn a_live_lease_of_another_pod_is_waited_for() {
        assert_eq!(
            decide(true, Some(OTHER), ME, Duration::from_secs(14)),
            Decision::Wait
        );
    }

    #[test]
    fn a_lease_not_renewed_for_its_duration_is_taken() {
        assert_eq!(
            decide(true, Some(OTHER), ME, Duration::from_secs(15)),
            Decision::Take
        );
    }

    #[test]
    fn the_leader_stops_before_a_follower_may_take_over() {
        // The last renewal attempt may still be running for RENEW_TIMEOUT.
        assert!(RENEW_DEADLINE + RENEW_TIMEOUT < Duration::from_secs(LEASE_SECONDS as u64));
    }

    #[test]
    fn a_renewal_keeps_the_acquire_time_and_the_transitions() {
        let mut first = lease_for(OTHER, None, false);
        first.metadata.resource_version = Some("41".into());
        let taken = lease_for(ME, Some(&first), false);
        assert_eq!(taken.metadata.resource_version.as_deref(), Some("41"));
        let spec = taken.spec.clone().unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some(ME));
        assert_eq!(spec.lease_duration_seconds, Some(LEASE_SECONDS));
        assert_eq!(spec.lease_transitions, Some(2));
        let renewed = lease_for(ME, Some(&taken), true);
        let renewed_spec = renewed.spec.unwrap();
        assert_eq!(renewed_spec.lease_transitions, Some(2));
        assert_eq!(renewed_spec.acquire_time, spec.acquire_time);
    }
}
