#!/usr/bin/env sh
# Polaris bootstrap: catalog -> principal -> principal-role -> catalog-role -> grants.
# Idempotent (create-or-skip on 409/ALREADY_EXISTS) with retry loops so compose
# ordering races self-heal (SPEC R012, E01/E04, NFR004). Exits 0 on re-run.
# Follows the upstream apache/polaris quickstart REST pattern (SPEC C1).
set -eu

# --- Config (from compose environment; sane quickstart defaults) ---
POLARIS_HOST="${POLARIS_HOST:-http://polaris:8181}"
POLARIS_HEALTH_URL="${POLARIS_HEALTH_URL:-http://polaris:8182/q/health}"
POLARIS_BOOTSTRAP_CREDENTIALS="${POLARIS_BOOTSTRAP_CREDENTIALS:?set POLARIS_BOOTSTRAP_CREDENTIALS (canonical triple REALM,clientId,clientSecret, e.g. POLARIS,root,<secret>; legacy clientId:clientSecret accepted)}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CATALOG="${POLARIS_CATALOG:-quickstart_catalog}"
POLARIS_PRINCIPAL="${POLARIS_PRINCIPAL:-quickstart_user}"
POLARIS_PRINCIPAL_ROLE="${POLARIS_PRINCIPAL_ROLE:-quickstart_principal_role}"
POLARIS_CATALOG_ROLE="${POLARIS_CATALOG_ROLE:-quickstart_catalog_role}"
S3_WAREHOUSE_LOCATION="${S3_WAREHOUSE_LOCATION:-s3://warehouse/}"
S3_ENDPOINT_INTERNAL="${S3_ENDPOINT_INTERNAL:-http://rustfs:9000}"
S3_ENDPOINT_EXTERNAL="${S3_ENDPOINT_EXTERNAL:-http://localhost:9000}"
AWS_REGION="${AWS_REGION:-us-east-1}"
POLARIS_S3_ROLE_ARN="${POLARIS_S3_ROLE_ARN:-arn:aws:iam::000000000000:role/polaris-quickstart}"
POLARIS_S3_STS_UNAVAILABLE="${POLARIS_S3_STS_UNAVAILABLE:-true}"

MAX_HEALTH_ATTEMPTS="${MAX_HEALTH_ATTEMPTS:-60}"   # 60 x 5s = ~5 min for JVM boot.
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"
CURL_RETRY="--retry 12 --retry-delay 5 --retry-all-errors"

log() { echo "[polaris-setup] $*"; }

# --- 1. Wait for Polaris health (self-heals E01 ordering races) ---
attempt=0
until curl -sf $CURL_RETRY --max-time 10 "$POLARIS_HEALTH_URL" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$MAX_HEALTH_ATTEMPTS" ]; then
    log "Polaris health check failed after $attempt attempts ($POLARIS_HEALTH_URL)"
    exit 1
  fi
  log "waiting for polaris ($attempt/$MAX_HEALTH_ATTEMPTS)..."
  sleep "$SLEEP_SECONDS"
done
log "Polaris is healthy."

# --- 2. OAuth2 client-credentials token (bootstrap root credential) ---
# Canonical contract is the comma-triple realm,clientId,clientSecret
# (e.g. POLARIS,root,<secret>); legacy clientId:clientSecret is accepted
# when the value contains no comma.
case "$POLARIS_BOOTSTRAP_CREDENTIALS" in
  *,*,*)
    _triple_rest="${POLARIS_BOOTSTRAP_CREDENTIALS#*,}"
    CLIENT_ID="${_triple_rest%%,*}"
    CLIENT_SECRET="${_triple_rest#*,}"
    unset _triple_rest
    ;;
  *:*)
    CLIENT_ID="${POLARIS_BOOTSTRAP_CREDENTIALS%%:*}"
    CLIENT_SECRET="${POLARIS_BOOTSTRAP_CREDENTIALS#*:}"
    ;;
  *)
    log "Invalid POLARIS_BOOTSTRAP_CREDENTIALS format: expected canonical triple REALM,clientId,clientSecret (e.g. POLARIS,root,<secret>); legacy clientId:clientSecret accepted."
    exit 1
    ;;
esac
if [ -z "${CLIENT_ID:-}" ] || [ -z "${CLIENT_SECRET:-}" ]; then
  log "Invalid POLARIS_BOOTSTRAP_CREDENTIALS format: empty client id or secret (expected REALM,clientId,clientSecret triple; legacy clientId:clientSecret accepted)."
  exit 1
fi
TOKEN_URL="$POLARIS_HOST/api/catalog/v1/oauth/tokens"
TOKEN=""
attempt=0
until TOKEN=$(curl -sf $CURL_RETRY --max-time 10 -u "$CLIENT_ID:$CLIENT_SECRET" \
    -d 'grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL' \
    "$TOKEN_URL" 2>/dev/null | jq -r '.access_token // empty') \
    && [ -n "$TOKEN" ]; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 12 ]; then
    log "Failed to obtain OAuth token from $TOKEN_URL"
    exit 1
  fi
  log "waiting for token endpoint ($attempt/12)..."
  sleep "$SLEEP_SECONDS"
done
log "OAuth token acquired."

MGMT="$POLARIS_HOST/api/management/v1"

# POST helper: 200/201 = created, 409 (or ALREADY_EXISTS body) = skip -> 0.
# Usage: api_post <url> <json-body> <label>
api_post() {
  url="$1"; body="$2"; label="$3"
  code=$(curl -s $CURL_RETRY --max-time 15 -o /tmp/polaris_resp.json -w '%{http_code}' \
    -X POST "$url" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Polaris-Realm: $POLARIS_REALM" \
    -H 'Content-Type: application/json' \
    -d "$body" 2>/dev/null || true)
  case "$code" in
    200|201) log "created $label." ;;
    400|409)
      if grep -qiE 'already.?exists|duplicate|conflict' /tmp/polaris_resp.json 2>/dev/null || [ "$code" = "409" ]; then
        log "$label already exists, skipping."
      else
        log "FAILED to create $label (HTTP $code): $(cat /tmp/polaris_resp.json 2>/dev/null)"
        return 1
      fi
      ;;
    *) log "FAILED to create $label (HTTP $code): $(cat /tmp/polaris_resp.json 2>/dev/null)"; return 1 ;;
  esac
}

# PUT helper (grants / role assignments): 200/201/204 = ok, 409 = skip -> 0.
api_put() {
  url="$1"; body="$2"; label="$3"
  if [ -z "$body" ] || [ "$body" = "-" ]; then
    code=$(curl -s $CURL_RETRY --max-time 15 -o /tmp/polaris_resp.json -w '%{http_code}' \
      -X PUT "$url" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Polaris-Realm: $POLARIS_REALM" \
      -H 'Content-Type: application/json' 2>/dev/null || true)
  else
    code=$(curl -s $CURL_RETRY --max-time 15 -o /tmp/polaris_resp.json -w '%{http_code}' \
      -X PUT "$url" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Polaris-Realm: $POLARIS_REALM" \
      -H 'Content-Type: application/json' \
      -d "$body" 2>/dev/null || true)
  fi
  case "$code" in
    200|201|204) log "granted $label." ;;
    409) log "$label already granted, skipping." ;;
    *) log "FAILED $label (HTTP $code): $(cat /tmp/polaris_resp.json 2>/dev/null)"; return 1 ;;
  esac
}

# --- 3. Bootstrap chain (create-or-skip each step; E04) ---
# Catalog probe matrix (SPEC R004/R005, PLAN P004-P005): the pinned
# apache/polaris:1.0.0-incubating validator settles the 1.0.0-tag drift
# (roleArn required on the tag, S3-compatible knobs only on main), so rows
# run in order P-a -> P-b -> P-c and the first 201 wins (winner logged):
#   P-a: base + well-formed dummy roleArn (tag-required field).
#   P-b: base as-is, no roleArn (wins when the runtime does not enforce it).
#   P-c: P-b shape with split endpoints (client-facing endpoint vs
#        server-side endpointInternal per the RustFS dual-endpoint rule).
# A 409 (or an already-exists body) on any row means a previous run already
# created the catalog -> skip the rest of the matrix (idempotent re-run).
# A retryable 400 advances to the next row; a 400 on the final row and any
# other unexpected code hard-fail with the last code/body logged.
case "$POLARIS_S3_STS_UNAVAILABLE" in
  true|false) ;;
  *) log "Invalid POLARIS_S3_STS_UNAVAILABLE: expected true/false, got '$POLARIS_S3_STS_UNAVAILABLE'"; exit 1 ;;
esac
# Usage: catalog_body <roleArn|no-roleArn> <unified|split>
catalog_body() {
  _role_mode="$1"; _ep_mode="$2"
  _ep="$S3_ENDPOINT_INTERNAL"; _epi="$S3_ENDPOINT_INTERNAL"
  if [ "$_ep_mode" = "split" ]; then _ep="$S3_ENDPOINT_EXTERNAL"; fi
  if [ "$_role_mode" = "roleArn" ]; then
    jq -n --arg name "$POLARIS_CATALOG" --arg loc "$S3_WAREHOUSE_LOCATION" \
      --arg ep "$_ep" --arg epi "$_epi" --arg region "$AWS_REGION" \
      --arg role "$POLARIS_S3_ROLE_ARN" --argjson sts "$POLARIS_S3_STS_UNAVAILABLE" \
      '{catalog: {name: $name, type: "INTERNAL",
        storageConfigInfo: {storageType: "S3", allowedLocations: [$loc],
          endpoint: $ep, endpointInternal: $epi, pathStyleAccess: true,
          region: $region, roleArn: $role, stsUnavailable: $sts},
        properties: {"default-base-location": $loc}}}'
  else
    jq -n --arg name "$POLARIS_CATALOG" --arg loc "$S3_WAREHOUSE_LOCATION" \
      --arg ep "$_ep" --arg epi "$_epi" --arg region "$AWS_REGION" \
      --argjson sts "$POLARIS_S3_STS_UNAVAILABLE" \
      '{catalog: {name: $name, type: "INTERNAL",
        storageConfigInfo: {storageType: "S3", allowedLocations: [$loc],
          endpoint: $ep, endpointInternal: $epi, pathStyleAccess: true,
          region: $region, stsUnavailable: $sts},
        properties: {"default-base-location": $loc}}}'
  fi
  unset _role_mode _ep_mode _ep _epi
}
for _row in P-a P-b P-c; do
  case "$_row" in
    P-a) _body="$(catalog_body roleArn unified)" ;;
    P-b) _body="$(catalog_body no-roleArn unified)" ;;
    P-c) _body="$(catalog_body no-roleArn split)" ;;
  esac
  _code=$(curl -s $CURL_RETRY --max-time 15 -o /tmp/polaris_resp.json -w '%{http_code}' \
    -X POST "$MGMT/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Polaris-Realm: $POLARIS_REALM" \
    -H 'Content-Type: application/json' \
    -d "$_body" 2>/dev/null || true)
  case "$_code" in
    200|201)
      log "catalog probe $_row won (HTTP $_code): created catalog $POLARIS_CATALOG."
      break
      ;;
    400|409)
      if grep -qiE 'already.?exists|duplicate|conflict' /tmp/polaris_resp.json 2>/dev/null || [ "$_code" = "409" ]; then
        log "catalog $POLARIS_CATALOG already exists, skipping."
        break
      elif [ "$_row" = "P-c" ]; then
        log "FAILED to create catalog $POLARIS_CATALOG (HTTP $_code): $(cat /tmp/polaris_resp.json 2>/dev/null)"
        exit 1
      else
        log "catalog probe $_row missed (HTTP $_code): $(cat /tmp/polaris_resp.json 2>/dev/null)"
      fi
      ;;
    *)
      log "FAILED to create catalog $POLARIS_CATALOG (HTTP $_code): $(cat /tmp/polaris_resp.json 2>/dev/null)"
      exit 1
      ;;
  esac
done
unset _row _body _code

api_post "$MGMT/principals" "$(jq -n --arg name "$POLARIS_PRINCIPAL" \
  '{name: $name, type: "USER"}')" \
  "principal $POLARIS_PRINCIPAL"

api_post "$MGMT/principal-roles" "$(jq -n --arg name "$POLARIS_PRINCIPAL_ROLE" \
  '{name: $name}')" \
  "principal-role $POLARIS_PRINCIPAL_ROLE"

api_post "$MGMT/catalogs/$POLARIS_CATALOG/catalog-roles" \
  "$(jq -n --arg name "$POLARIS_CATALOG_ROLE" '{name: $name}')" \
  "catalog-role $POLARIS_CATALOG_ROLE"

api_put "$MGMT/catalogs/$POLARIS_CATALOG/catalog-roles/$POLARIS_CATALOG_ROLE/grants" \
  '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' \
  "CATALOG_MANAGE_CONTENT on $POLARIS_CATALOG to $POLARIS_CATALOG_ROLE"

api_put "$MGMT/principals/$POLARIS_PRINCIPAL/principal-roles" \
  "$(jq -n --arg name "$POLARIS_PRINCIPAL_ROLE" '{principalRole: {name: $name}}')" \
  "principal-role $POLARIS_PRINCIPAL_ROLE to principal $POLARIS_PRINCIPAL"

api_put "$MGMT/principal-roles/$POLARIS_PRINCIPAL_ROLE/catalog-roles/$POLARIS_CATALOG" \
  "$(jq -n --arg name "$POLARIS_CATALOG_ROLE" '{catalogRole: {name: $name}}')" \
  "catalog-role $POLARIS_CATALOG_ROLE to principal-role $POLARIS_PRINCIPAL_ROLE"

log "BOOTSTRAP OK"
