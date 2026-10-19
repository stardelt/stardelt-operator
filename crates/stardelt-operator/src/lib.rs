//! stardelt-operator — the CRD reconciler that composes upstream best-of-breed
//! projects into one declarative experience.
//!
//! The CRD types in [`crd`] are the plug specification; the reconciler turns a
//! small set of top-level resources into per-service operators and Helm
//! releases. See the master spec §3 and §5.

pub mod controller;
pub mod crd;
pub mod helm;
pub mod identity;
pub mod objects;
pub mod planner;
pub mod prerequisites;
pub mod provision;
pub mod values;
