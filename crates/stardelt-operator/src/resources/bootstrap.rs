//! The `lakekeeper-bootstrap` Job.
//!
//! Accepts Lakekeeper's terms-of-use and creates the default `warehouse`
//! pointing at SeaweedFS S3. Idempotent: skips bootstrap if already done, skips
//! warehouse creation if it already exists. Ported from
//! `stardelt-platform/manifests/lakekeeper-bootstrap.yaml`.

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::api::PlatformInstance;
use crate::resources::secret::SECRET_NAME;

pub const JOB_NAME: &str = "lakekeeper-bootstrap";

/// In-cluster Lakekeeper management URL.
fn lakekeeper_url(namespace: &str) -> String {
    format!("http://lakekeeper.{namespace}.svc.cluster.local:8181")
}

const SCRIPT: &str = r#"set -e
echo "▶ checking Lakekeeper info"
INFO=$(curl -fsS "$LK/management/v1/info")
echo "$INFO"
BOOTSTRAPPED=$(echo "$INFO" | grep -o '"bootstrapped":[^,}]*' | cut -d: -f2 | tr -d ' "')

if [ "$BOOTSTRAPPED" = "true" ]; then
  echo "✓ already bootstrapped"
else
  echo "▶ bootstrapping"
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d '{"accept-terms-of-use":true,"is-operator":true}' \
    "$LK/management/v1/bootstrap"
  echo
  echo "✓ bootstrapped"
fi

echo "▶ checking for existing 'warehouse'"
EXISTING=$(curl -fsS "$LK/management/v1/warehouse" || echo '{"warehouses":[]}')
echo "$EXISTING"
if echo "$EXISTING" | grep -q '"name":"warehouse"'; then
  echo "✓ warehouse 'warehouse' already exists"
  exit 0
fi

echo "▶ creating warehouse 'warehouse' on s3://$S3_BUCKET ($S3_ENDPOINT)"
cat > /tmp/wh.json <<EOF
{
  "warehouse-name": "warehouse",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "$S3_BUCKET",
    "key-prefix": "warehouse",
    "endpoint": "$S3_ENDPOINT",
    "region": "$S3_REGION",
    "path-style-access": true,
    "flavor": "minio",
    "sts-enabled": false,
    "remote-signing-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "$S3_ACCESS_KEY",
    "aws-secret-access-key": "$S3_SECRET_KEY"
  }
}
EOF
cat /tmp/wh.json
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d @/tmp/wh.json \
  "$LK/management/v1/warehouse"
echo
echo "✓ warehouse created"
"#;

pub fn build(pi: &PlatformInstance, owner: OwnerReference) -> Job {
    let ns = &pi.spec.namespace;
    let secret_env = |env_name: &str, key: &str| {
        serde_json::json!({
            "name": env_name,
            "valueFrom": { "secretKeyRef": { "name": SECRET_NAME, "key": key } }
        })
    };

    // Build the pod spec as JSON then deserialize — far less verbose than the
    // k8s-openapi builder structs for a one-off Job, and validated at apply.
    let job_json = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": JOB_NAME,
            "namespace": ns,
            "labels": super::labels(),
            "ownerReferences": [owner_to_json(&owner)],
        },
        "spec": {
            "ttlSecondsAfterFinished": 600,
            "backoffLimit": 4,
            "template": {
                "spec": {
                    "restartPolicy": "OnFailure",
                    "containers": [{
                        "name": "bootstrap",
                        "image": "curlimages/curl:8.10.1",
                        "command": ["/bin/sh", "-c"],
                        "args": [SCRIPT],
                        "env": [
                            { "name": "LK", "value": lakekeeper_url(ns) },
                            secret_env("S3_ENDPOINT", "endpoint"),
                            secret_env("S3_BUCKET", "bucket"),
                            secret_env("S3_REGION", "region"),
                            secret_env("S3_ACCESS_KEY", "access-key"),
                            secret_env("S3_SECRET_KEY", "secret-key"),
                        ],
                    }],
                }
            }
        }
    });

    serde_json::from_value(job_json).expect("static bootstrap Job JSON is valid")
}

/// Serialize an `OwnerReference` to JSON for embedding in a json! body.
fn owner_to_json(owner: &OwnerReference) -> serde_json::Value {
    serde_json::to_value(owner).expect("OwnerReference serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_job_builds() {
        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": { "namespace": "lake" }
        }))
        .unwrap();
        let job = build(&pi, OwnerReference::default());
        assert_eq!(job.metadata.name.as_deref(), Some(JOB_NAME));
        assert_eq!(job.metadata.namespace.as_deref(), Some("lake"));
        // The container env must reference the S3 creds Secret.
        let spec = job.spec.unwrap().template.spec.unwrap();
        let env = spec.containers[0].env.as_ref().unwrap();
        assert!(env.iter().any(|e| e.name == "S3_ACCESS_KEY"));
        assert!(env.iter().any(|e| e.name == "LK"));
    }
}
