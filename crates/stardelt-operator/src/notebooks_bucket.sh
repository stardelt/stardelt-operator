set -e
# Creates the versioned bucket that Nova's notebook publishing writes to, and checks that
# versioning is on. Safe to run again: an existing bucket is kept as it is.
s3() { aws --endpoint-url "$AWS_ENDPOINT_URL" "$@"; }
# The gateway can still be starting right after an install: wait before deciding the bucket is absent.
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
s3 s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled
state="$(s3 s3api get-bucket-versioning --bucket "$BUCKET" --query Status --output text)"
[ "$state" = Enabled ] || { echo "versioning on $BUCKET is $state" >&2; exit 1; }
echo "bucket $BUCKET: versioning $state"
