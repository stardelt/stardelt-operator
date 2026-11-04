set -e
# Creates the destination bucket of the object-store archive (platform#462), the copy the
# `lakehouse-backup` CronJob syncs into. No versioning: the sync is append-only (no --delete),
# so a second copy of every changed object would only cost space. Safe to run again: an existing
# bucket is kept as it is.
s3() { aws --endpoint-url "$AWS_ENDPOINT_URL" "$@"; }
n=0
until s3 s3api list-buckets >/dev/null 2>&1; do
  n=$((n + 1))
  if [ "$n" -ge 30 ]; then
    echo "object store at $AWS_ENDPOINT_URL is not reachable" >&2
    exit 1
  fi
  sleep 2
done
if ! s3 s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  s3 s3api create-bucket --bucket "$BUCKET" >/dev/null
fi
s3 s3api head-bucket --bucket "$BUCKET"
echo "bucket $BUCKET: present (no versioning: the archive is append-only)"
