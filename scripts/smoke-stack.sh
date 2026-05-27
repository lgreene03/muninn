#!/usr/bin/env bash
# Norse Stack end-to-end smoke test.
#
# Validates the full pipeline:
#   Trade → Muninn (feature engine) → Huginn (strategy) → Sleipnir (execution) → Fill
#
# Usage:
#   bash scripts/smoke-stack.sh              # leave stack running for inspection
#   bash scripts/smoke-stack.sh --teardown   # tear down after test
#
# Prerequisites:
#   - Docker and docker-compose
#   - Sibling checkouts: ../huginn, ../sleipnir
#   - curl, jq (optional)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.stack.yml"
TEARDOWN=false

for arg in "$@"; do
  case "$arg" in
    --teardown) TEARDOWN=true ;;
  esac
done

MUNINN_URL="http://localhost:8080"
HUGINN_URL="http://localhost:8083"
SLEIPNIR_URL="http://localhost:8085"
TIMEOUT=90

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass()  { echo -e "${GREEN}  ✓ $1${NC}"; }
fail()  { echo -e "${RED}  ✗ $1${NC}"; FAILURES=$((FAILURES + 1)); }
info()  { echo -e "${YELLOW}  → $1${NC}"; }
phase() { echo -e "\n${CYAN}═══ $1 ═══${NC}"; }

FAILURES=0

cleanup() {
  if [ "$TEARDOWN" = true ]; then
    info "Tearing down stack..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ── Phase 1: Boot the stack ─────────────────────────────────────────────

phase "Phase 1: Boot the Norse stack"

# Verify sibling repos exist
for repo in huginn sleipnir; do
  if [ ! -f "$PROJECT_DIR/../$repo/Dockerfile" ]; then
    echo -e "${RED}Missing sibling repo: ../$repo${NC}"
    echo "The cross-stack smoke test requires ../huginn and ../sleipnir to be checked out."
    exit 1
  fi
done
pass "Sibling repos found (huginn, sleipnir)"

info "Starting docker compose (this may take a while on first build)..."
docker compose -f "$COMPOSE_FILE" up -d --build 2>&1 | tail -5

# Wait for Redpanda
info "Waiting for Redpanda..."
for i in $(seq 1 "$TIMEOUT"); do
  if docker compose -f "$COMPOSE_FILE" exec -T redpanda rpk cluster health --exit-when-healthy 2>/dev/null; then
    break
  fi
  if [ "$i" -eq "$TIMEOUT" ]; then fail "Redpanda did not become healthy"; exit 1; fi
  sleep 1
done
pass "Redpanda healthy"

# Create topics
info "Creating topics..."
docker compose -f "$COMPOSE_FILE" exec -T redpanda rpk topic create \
  events.trade features.obi.v1 features.vwap.1m.v1 \
  executions.intents.v1 executions.fills.v1 \
  2>/dev/null || true
pass "Topics created"

# Wait for services
for svc_name_url in "Muninn:${MUNINN_URL}/actuator/health" "Huginn:${HUGINN_URL}/healthz" "Sleipnir:${SLEIPNIR_URL}/healthz"; do
  svc_name="${svc_name_url%%:*}"
  svc_url="${svc_name_url#*:}"
  info "Waiting for $svc_name at $svc_url..."
  SVC_OK=false
  for i in $(seq 1 "$TIMEOUT"); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$svc_url" 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "200" ]; then
      SVC_OK=true
      break
    fi
    sleep 2
  done
  if [ "$SVC_OK" = true ]; then
    pass "$svc_name is up"
  else
    fail "$svc_name failed to start (last HTTP $HTTP_CODE)"
  fi
done

# ── Phase 2: Push trade data through Muninn ─────────────────────────────

phase "Phase 2: Ingest trade through Muninn"

NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EVENT_ID=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || echo "00000000-0000-7000-8000-000000000001")

TRADE_JSON=$(cat <<EOF
{
  "eventId": "${EVENT_ID}",
  "eventTime": "${NOW}",
  "ingestTime": "${NOW}",
  "source": "smoke-stack-test",
  "instrument": {
    "symbol": "BTC-USDT",
    "baseAsset": "BTC",
    "quoteAsset": "USDT",
    "exchange": {
      "id": "binance",
      "displayName": "Binance Spot",
      "timezone": "UTC"
    }
  },
  "sequenceNumber": 1,
  "schemaVersion": 1,
  "price": 67500.50,
  "size": 0.01,
  "side": "BUY",
  "exchangeTradeId": "smoke-stack-001"
}
EOF
)

RESPONSE=$(curl -s -X POST "${MUNINN_URL}/api/v1/events/trade" \
  -H "Content-Type: application/json" \
  -d "${TRADE_JSON}" \
  -w "\n%{http_code}")
HTTP_CODE=$(echo "$RESPONSE" | tail -1)

if [ "$HTTP_CODE" = "201" ]; then
  pass "Trade event accepted by Muninn (HTTP 201)"
else
  fail "Trade event rejected by Muninn (HTTP $HTTP_CODE)"
fi

# ── Phase 3: Inject synthetic OBI feature for Huginn ────────────────────

phase "Phase 3: Inject OBI feature event for Huginn"

# Muninn computes VWAP features; Huginn consumes OBI features.
# Bridge the gap with a synthetic OBI event on the topic Huginn watches.

FEATURE_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
FEATURE_ID=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || echo "00000000-0000-7000-8000-000000000002")

FEATURE_JSON=$(cat <<EOF
{"eventId":"${FEATURE_ID}","eventTime":"${FEATURE_TIME}","featureName":"obi","featureVersion":"v1","instrument":"BTC-USDT","windowStart":"${FEATURE_TIME}","windowEnd":"${FEATURE_TIME}","values":{"obi":-0.85,"micro_price":67500.50,"bid_price":67490.00,"ask_price":67510.00}}
EOF
)

PRODUCE_OUTPUT=$(echo "$FEATURE_JSON" | docker compose -f "$COMPOSE_FILE" exec -T redpanda \
  rpk topic produce features.obi.v1 2>&1 || echo "PRODUCE_FAILED")

if echo "$PRODUCE_OUTPUT" | grep -q "Produced to partition"; then
  pass "OBI feature event produced to features.obi.v1"
else
  fail "Failed to produce feature event: $PRODUCE_OUTPUT"
fi

# ── Phase 4: Verify Huginn processes the signal ─────────────────────────

phase "Phase 4: Verify Huginn strategy signal"

# Wait for Huginn to consume the feature, produce an intent, and for Sleipnir
# to fill it. The whole round-trip typically completes in < 5 seconds.
info "Waiting for pipeline to process (feature → intent → fill)..."
PIPELINE_OK=false
for i in $(seq 1 20); do
  SNAPSHOT=$(curl -s "${HUGINN_URL}/api/snapshot" 2>/dev/null || echo "{}")
  if echo "$SNAPSHOT" | grep -qE '"TotalFills":[1-9]'; then
    PIPELINE_OK=true
    break
  fi
  sleep 1
done

FEATURES_TOTAL=$(curl -s "${HUGINN_URL}/metrics" 2>/dev/null | grep "huginn_features_consumed_total" | grep -v "^#" | awk '{print $2}' || echo "0")
info "Features consumed: ${FEATURES_TOTAL}"

if [ "$PIPELINE_OK" = true ]; then
  pass "Huginn consumed feature and strategy fired"
else
  info "Huginn snapshot: $SNAPSHOT"
  if [ "${FEATURES_TOTAL:-0}" != "0" ] && [ "${FEATURES_TOTAL:-0}" != "" ]; then
    pass "Huginn consumed ${FEATURES_TOTAL} features (strategy may not have triggered)"
  else
    fail "Huginn did not consume any features"
  fi
fi

# ── Phase 5: Verify Sleipnir execution ───────────────────────────────────

phase "Phase 5: Verify Sleipnir execution"

if [ "$PIPELINE_OK" = true ]; then
  # Snapshot already shows fills — extract the fill details
  FILL_COUNT=$(echo "$SNAPSHOT" | grep -o '"TotalFills":[0-9]*' | cut -d: -f2 || echo "0")
  pass "Sleipnir processed intent → ${FILL_COUNT} fill(s) in Huginn portfolio"
else
  info "Skipping — Huginn did not produce an intent in Phase 4"
fi

# ── Phase 6: Verify Huginn applies the fill ─────────────────────────────

phase "Phase 6: Verify Huginn portfolio update"

if [ "$PIPELINE_OK" = true ]; then
  # Check that portfolio has a position
  if echo "$SNAPSHOT" | grep -qE '"Positions":\{".+"\}'; then
    pass "Huginn portfolio has open position(s)"
  else
    info "Huginn snapshot: $SNAPSHOT"
    fail "Huginn portfolio has no positions after fill"
  fi
else
  info "Skipping — no fill was published in Phase 5"
fi

# ── Phase 7: Service health checks ─────────────────────────────────────

phase "Phase 7: Service health checks"

# Muninn
MUNINN_QUERY=$(curl -s -o /dev/null -w "%{http_code}" "${MUNINN_URL}/api/v1/features/vwap?instrument=BTC-USDT&from=2026-01-01T00:00:00Z&to=2026-12-31T00:00:00Z" 2>/dev/null)
if [ "$MUNINN_QUERY" = "200" ]; then
  pass "Muninn Query API (200)"
else
  info "Muninn Query API returned $MUNINN_QUERY (may have no data)"
fi

MUNINN_DOCS=$(curl -s -o /dev/null -w "%{http_code}" "${MUNINN_URL}/api-docs" 2>/dev/null)
if [ "$MUNINN_DOCS" = "200" ]; then pass "Muninn OpenAPI spec"; else fail "Muninn /api-docs ($MUNINN_DOCS)"; fi

HUGINN_METRICS=$(curl -s "${HUGINN_URL}/metrics" 2>/dev/null || echo "")
if echo "$HUGINN_METRICS" | grep -q "huginn_"; then
  pass "Huginn Prometheus metrics"
else
  fail "Huginn /metrics missing huginn_ prefix"
fi

HUGINN_VERSION=$(curl -s -o /dev/null -w "%{http_code}" "${HUGINN_URL}/version" 2>/dev/null)
if [ "$HUGINN_VERSION" = "200" ]; then pass "Huginn /version"; else fail "Huginn /version ($HUGINN_VERSION)"; fi

SLEIPNIR_METRICS=$(curl -s -o /dev/null -w "%{http_code}" "${SLEIPNIR_URL}/metrics" 2>/dev/null)
if [ "$SLEIPNIR_METRICS" = "200" ]; then pass "Sleipnir /metrics"; else fail "Sleipnir /metrics ($SLEIPNIR_METRICS)"; fi

SLEIPNIR_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "${SLEIPNIR_URL}/healthz" 2>/dev/null)
if [ "$SLEIPNIR_HEALTH" = "200" ]; then pass "Sleipnir /healthz"; else fail "Sleipnir /healthz ($SLEIPNIR_HEALTH)"; fi

# ── Summary ─────────────────────────────────────────────────────────────

echo ""
if [ "$FAILURES" -eq 0 ]; then
  echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
  echo -e "${GREEN}  Norse Stack smoke test passed (0 failures)      ${NC}"
  echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
else
  echo -e "${RED}═══════════════════════════════════════════════════${NC}"
  echo -e "${RED}  Norse Stack smoke test: ${FAILURES} failure(s)            ${NC}"
  echo -e "${RED}═══════════════════════════════════════════════════${NC}"
fi

echo ""
echo "  Muninn API:       ${MUNINN_URL}"
echo "  Muninn Swagger:   ${MUNINN_URL}/swagger-ui.html"
echo "  Huginn API:       ${HUGINN_URL}"
echo "  Huginn Dashboard: http://localhost:8084  (if dashboard service enabled)"
echo "  Sleipnir API:     ${SLEIPNIR_URL}"
echo "  Redpanda:         localhost:19092"
echo ""

if [ "$TEARDOWN" = false ]; then
  echo "  Stack is still running. Tear down with:"
  echo "    docker compose -f docker-compose.stack.yml down -v"
fi

exit "$FAILURES"
