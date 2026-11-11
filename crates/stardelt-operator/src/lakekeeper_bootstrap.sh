set -e
umask 077
# The catalog's own identity (ServicePrincipal catalog-operator, operator#194). Optional: a
# Lakehouse in another namespace, or a platform without an IdP, mounts nothing and stays
# anonymous exactly as before.
CRED_DIR=/var/run/secrets/catalog-principal
TOKEN_FILE=/tmp/catalog-token.json
AUTH_FILE=/tmp/catalog-auth-header
GRANT_FILE=/tmp/grant.json
GRANT_OUT=/tmp/grant-out.json
trap 'rm -f "$TOKEN_FILE" "$AUTH_FILE" "$GRANT_FILE" "$GRANT_OUT" /tmp/cred.json /tmp/wh.json' EXIT
AUTH_ARGS=""
if [ -s "$CRED_DIR/client-id" ] && [ -s "$CRED_DIR/client-secret" ] && [ -s "$CRED_DIR/token-url" ]; then
  echo "▶ requesting a client-credentials token for $(cat "$CRED_DIR/client-id")"
  curl -fsS -X POST "$(cat "$CRED_DIR/token-url")" \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode client_id@"$CRED_DIR/client-id" \
    --data-urlencode client_secret@"$CRED_DIR/client-secret" \
    -o "$TOKEN_FILE" || {
      echo "the realm's token endpoint refused $(cat "$CRED_DIR/client-id")" >&2
      exit 1
    }
  TOKEN=$(sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$TOKEN_FILE" | head -n 1)
  [ -n "$TOKEN" ] || { echo "the token response carried no access_token" >&2; exit 1; }
  printf 'Authorization: Bearer %s\n' "$TOKEN" > "$AUTH_FILE"
  AUTH_ARGS="-H @$AUTH_FILE"
  echo "✓ token acquired for $(cat "$CRED_DIR/client-id")"
else
  echo "▶ no catalog principal mounted — calling the management API anonymously"
fi
echo "▶ checking Lakekeeper info"
INFO=$(curl -fsS $AUTH_ARGS "$LK/management/v1/info")
echo "$INFO"
BOOTSTRAPPED=$(echo "$INFO" | grep -o '"bootstrapped":[^,}]*' | cut -d: -f2 | tr -d ' "')

if [ "$BOOTSTRAPPED" = "true" ]; then
  echo "✓ already bootstrapped"
else
  echo "▶ bootstrapping"
  curl -fsS $AUTH_ARGS -X POST -H 'Content-Type: application/json' \
    -d '{"accept-terms-of-use":true,"is-operator":true}' \
    "$LK/management/v1/bootstrap"
  echo
  echo "✓ bootstrapped"
fi

# grant <project role> <secret dir> — writes one project assignment for the directory's
# subject. Missing subject: nothing to grant. 409: already present.
grant() {
  TYPE=$1
  DIR=$2
  [ -s "$DIR/subject" ] || return 0
  SUBJECT=$(cat "$DIR/subject")
  echo "▶ granting $TYPE to oidc~$SUBJECT"
  printf '{"writes":[{"type":"%s","user":"oidc~%s"}],"deletes":[]}' "$TYPE" "$SUBJECT" > "$GRANT_FILE"
  CODE=$(curl -sS -o "$GRANT_OUT" -w '%{http_code}' $AUTH_ARGS -X POST \
    -H 'Content-Type: application/json' --data @"$GRANT_FILE" \
    "$LK/management/v1/permissions/project/assignments")
  case "$CODE" in
    200|204|409) echo "✓ $TYPE present for oidc~$SUBJECT" ;;
    *)
      echo "grant $TYPE returned $CODE: $(head -c 300 "$GRANT_OUT" 2>/dev/null || true)" >&2
      exit 1
      ;;
  esac
}

AUTHZ=$(echo "$INFO" | grep -o '"authz-backend":"[^"]*"' | cut -d'"' -f4)
if [ "$AUTHZ" = "openfga" ]; then
  grant project_admin "$CRED_DIR"
  grant data_admin /var/run/secrets/catalog-trino
else
  echo "✓ project grants skipped (authz backend ${AUTHZ:-unknown})"
fi

echo "▶ checking for existing 'warehouse'"
EXISTING=$(curl -fsS $AUTH_ARGS "$LK/management/v1/warehouse" || echo '{"warehouses":[]}')
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
  curl -fsS $AUTH_ARGS -X POST -H 'Content-Type: application/json' \
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
curl -fsS $AUTH_ARGS -X POST -H 'Content-Type: application/json' \
  -d @/tmp/wh.json \
  "$LK/management/v1/warehouse"
echo
echo "✓ warehouse created"
