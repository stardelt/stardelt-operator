//! Pure reconcile planning. Separating the *plan* (which components a resource
//! needs) from the *apply* (server-side apply of upstream charts) keeps the
//! interesting logic unit-testable without a cluster.

use crate::crd::{CatalogImpl, LakehouseSpec, StorageImpl};

#[derive(Debug, PartialEq, Eq)]
pub struct ComponentPlan {
    /// Stable component name (also the status key).
    pub name: &'static str,
    /// Upstream Helm chart reference, if installed via Helm.
    pub chart: Option<&'static str>,
}

/// Which components a `Lakehouse` needs, in apply order.
///
/// Storage first (S3 must exist), then the catalog's database, then the catalog.
pub fn plan_lakehouse(spec: &LakehouseSpec) -> Vec<ComponentPlan> {
    let mut plan = Vec::new();

    if spec.storage.implementation == StorageImpl::Seaweedfs {
        plan.push(ComponentPlan { name: "seaweedfs", chart: Some("seaweedfs/seaweedfs") });
    }

    plan.push(ComponentPlan { name: "cloudnative-pg", chart: Some("cnpg/cloudnative-pg") });

    match spec.catalog.implementation {
        CatalogImpl::Lakekeeper => {
            plan.push(ComponentPlan { name: "lakekeeper", chart: Some("lakekeeper/lakekeeper") })
        }
        CatalogImpl::Polaris => {
            plan.push(ComponentPlan { name: "polaris", chart: Some("polaris/polaris") })
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{CatalogSpec, LakehouseSpec, StorageSpec};

    fn spec(catalog: CatalogImpl, storage: StorageImpl) -> LakehouseSpec {
        LakehouseSpec {
            catalog: CatalogSpec { implementation: catalog },
            storage: StorageSpec { implementation: storage, bucket: None },
            profile: Default::default(),
        }
    }

    #[test]
    fn default_plan_is_seaweedfs_cnpg_lakekeeper() {
        let plan = plan_lakehouse(&spec(CatalogImpl::Lakekeeper, StorageImpl::Seaweedfs));
        let names: Vec<_> = plan.iter().map(|c| c.name).collect();
        assert_eq!(names, vec!["seaweedfs", "cloudnative-pg", "lakekeeper"]);
    }

    #[test]
    fn byos3_skips_seaweedfs_and_polaris_replaces_lakekeeper() {
        let plan = plan_lakehouse(&spec(CatalogImpl::Polaris, StorageImpl::ByoS3));
        let names: Vec<_> = plan.iter().map(|c| c.name).collect();
        assert_eq!(names, vec!["cloudnative-pg", "polaris"]);
    }
}
