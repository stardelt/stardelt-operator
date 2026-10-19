set -e
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
  echo "✓ warehouse 'warehouse' already exists — refreshing its storage credential"
  WH_ID=$(echo "$EXISTING" | grep -o '"id":"[^"]*","warehouse-id":"[^"]*","name":"warehouse"' | head -1 | cut -d'"' -f4)
  if [ -z "$WH_ID" ]; then
    echo "could not read the id of warehouse 'warehouse'" >&2
    exit 1
  fi
  # Same care as below: the payload carries the S3 secret key.
  umask 077
  trap 'rm -f /tmp/cred.json' EXIT
  cat > /tmp/cred.json <<EOF
  {
    "new-storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "$S3_ACCESS_KEY",
      "aws-secret-access-key": "$S3_SECRET_KEY"
    }
  }
EOF
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d @/tmp/cred.json \
    "$LK/management/v1/warehouse/$WH_ID/storage-credential"
  echo
  echo "✓ storage credential refreshed"
  exit 0
fi

echo "▶ creating warehouse 'warehouse' on s3://$S3_BUCKET ($S3_ENDPOINT)"
# The payload carries the S3 secret key: keep the file private, remove it
# on exit, and never print it (job logs are readable by anyone with pods/log).
umask 077
trap 'rm -f /tmp/wh.json' EXIT
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
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d @/tmp/wh.json \
  "$LK/management/v1/warehouse"
echo
echo "✓ warehouse created"
