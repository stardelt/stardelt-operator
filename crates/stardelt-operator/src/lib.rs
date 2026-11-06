//! stardelt-operator — the CRD reconciler that composes upstream best-of-breed
//! projects into one declarative experience.
//!
//! The CRD types in [`crd`] are the plug specification; the reconciler turns a
//! small set of top-level resources into per-service operators and Helm
//! releases. See the master spec §3 and §5.

pub mod access_sync;
pub mod airflow;
pub mod connection;
pub mod controller;
pub mod crd;
pub mod helm;
pub mod identity;
pub mod identity_checklist;
pub mod ingress;
pub mod keycloak;
pub mod network_policy;
pub mod nova;
pub mod objects;
pub mod pg_dump;
pub mod planner;
pub mod prerequisites;
pub mod principal;
pub mod principal_keycloak;
pub mod provision;
pub mod secrets;
pub mod spark;
pub mod superset;
pub mod values;
