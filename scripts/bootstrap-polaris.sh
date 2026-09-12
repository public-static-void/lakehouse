#!/usr/bin/env sh
# Polaris bootstrap: catalog -> principal -> principal-role -> catalog-role -> grants.
# Idempotent (create-or-skip on 409/ALREADY_EXISTS) with retry loops so compose
# ordering races self-heal (SPEC R012, E01/E04, NFR004). Exits 0 on re-run.
# Follows the upstream apache/polaris quickstart REST pattern (SPEC C1).
set -eu

# --- Config (from compose environment; sane quickstart defaults) ---
POLARIS_HOST="${POLARIS_HOST:-http://polaris:8181}"
POLARIS_HEALTH_URL="${POLARIS_HEALTH_URL:-http://polaris:8182/q/health}"
POLARIS_BOOTSTRAP_CREDENTIALS="${POLARIS_BOOTSTRAP_CREDENTIALS:?set POLARIS_BOOTSTRAP_CREDENTIALS (id:secret)}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CATALOG="${POLARIS_CATALOG:-quickstart_catalog}"
POLARIS_PRINCIPAL="${POLARIS_PRINCIPAL:-quickstart_user}"
POLARIS_PRINCIPAL_ROLE="${POLARIS_PRINCIPAL_ROLE:-quickstart_principal_role}"
POLARIS_CATALOG_ROLE="${POLARIS_CATALOG_ROLE:-quickstart_catalog_role}"
S3_WAREHOUSE_LOCATION="${S3_WAREHOUSE_LOCATION:-s3://warehouse/}"
S3_ENDPOINT_INTERNAL="${S3_ENDPOINT_INTERNAL:-http://rustfs:9000}"

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
CLIENT_ID="${POLARIS_BOOTSTRAP_CREDENTIALS%%:*}"
CLIENT_SECRET="${POLARIS_BOOTSTRAP_CREDENTIALS#*:}"
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
    400|409) log "$label already granted, skipping." ;;
    *) log "FAILED $label (HTTP $code): $(cat /tmp/polaris_resp.json 2>/dev/null)"; return 1 ;;
  esac
}

# --- 3. Bootstrap chain (create-or-skip each step; E04) ---
api_post "$MGMT/catalogs" "$(jq -n \
  --arg name "$POLARIS_CATALOG" \
  --arg loc "$S3_WAREHOUSE_LOCATION" \
  --arg ep "$S3_ENDPOINT_INTERNAL" \
  '{name: $name, type: "INTERNAL",
    storageConfigInfo: {storageType: "S3", allowedLocations: [$loc],
      s3: {endpoint: $ep, pathStyleAccess: true}},
    properties: {"s3.endpoint": $ep, "s3.path-style-access": "true"}}')" \
  "catalog $POLARIS_CATALOG"

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
  '{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}' \
  "CATALOG_MANAGE_CONTENT on $POLARIS_CATALOG to $POLARIS_CATALOG_ROLE"

api_put "$MGMT/principal-roles/$POLARIS_PRINCIPAL_ROLE/principals/$POLARIS_PRINCIPAL" \
  "-" "principal-role $POLARIS_PRINCIPAL_ROLE to principal $POLARIS_PRINCIPAL"

api_put "$MGMT/principal-roles/$POLARIS_PRINCIPAL_ROLE/catalog-roles/$POLARIS_CATALOG/catalog-roles/$POLARIS_CATALOG_ROLE" \
  "-" "catalog-role $POLARIS_CATALOG_ROLE to principal-role $POLARIS_PRINCIPAL_ROLE"

log "BOOTSTRAP OK"
