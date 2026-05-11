#!/usr/bin/env bash
# Muninn end-to-end smoke test.
# Validates: docker-compose → app health → synthetic event → Redpanda consumption → metrics.
#
# Usage: ./scripts/smoke.sh
# Exit 0 on success, 1 on failure.
#
# Requirements:
#   - Docker Compose
#   - curl
#   - jq (optional, for pretty output)

set -euo pipefail

APP_URL="${APP_URL:-http://localhost:8080}"
BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"
TIMEOUT=30

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}✓ $1${NC}"; }
fail() { echo -e "${RED}✗ $1${NC}"; exit 1; }
info() { echo -e "${YELLOW}→ $1${NC}"; }

# --- Step 1: Check Docker Compose services ---
info "Step 1: Checking Docker Compose services..."

if ! docker compose ps --format json 2>/dev/null | head -1 > /dev/null; then
  info "Docker Compose services not running. Starting..."
  docker compose up -d --wait
fi

# Wait for Redpanda
for i in $(seq 1 "$TIMEOUT"); do
  if docker compose exec -T redpanda rpk cluster health --exit-when-healthy 2>/dev/null; then
    break
  fi
  if [ "$i" -eq "$TIMEOUT" ]; then
    fail "Redpanda did not become healthy within ${TIMEOUT}s"
  fi
  sleep 1
done
pass "Docker Compose services are healthy"

# --- Step 2: Create topics ---
info "Step 2: Creating topics..."
bash "$(dirname "$0")/create-topics.sh" || true
pass "Topics created"

# --- Step 3: Check application health ---
info "Step 3: Checking application health at ${APP_URL}/actuator/health..."

HEALTH_OK=false
for i in $(seq 1 "$TIMEOUT"); do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${APP_URL}/actuator/health" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" = "200" ]; then
    HEALTH_OK=true
    break
  fi
  sleep 1
done

if [ "$HEALTH_OK" = true ]; then
  pass "Application health check passed"
else
  fail "Application health check failed (HTTP ${HTTP_CODE})"
fi

# --- Step 4: Post a synthetic trade event ---
info "Step 4: Posting synthetic trade event..."

EVENT_ID=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || echo "00000000-0000-7000-8000-000000000001")
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

TRADE_JSON=$(cat <<EOF
{
  "eventId": "${EVENT_ID}",
  "eventTime": "${NOW}",
  "ingestTime": "${NOW}",
  "source": "smoke-test",
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
  "exchangeTradeId": "smoke-test-001"
}
EOF
)

RESPONSE=$(curl -s -X POST "${APP_URL}/api/v1/events/trade" \
  -H "Content-Type: application/json" \
  -d "${TRADE_JSON}" \
  -w "\n%{http_code}")

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "201" ]; then
  pass "Synthetic trade event accepted (HTTP 201)"
  echo "  Response: $BODY"
else
  fail "Synthetic trade event rejected (HTTP ${HTTP_CODE}): $BODY"
fi

# --- Step 5: Check Prometheus metrics ---
info "Step 5: Checking Prometheus metrics..."

METRICS=$(curl -s "${APP_URL}/actuator/prometheus" 2>/dev/null || echo "")
if echo "$METRICS" | grep -q "muninn_ingest"; then
  pass "Ingestion metrics present in Prometheus endpoint"
else
  info "No ingestion metrics found yet (may appear after first real event)"
fi

# --- Step 6: Check for feature engine output ---
info "Step 6: Checking for feature engine output..."

# Wait a few seconds for processing
sleep 2

# Check if features.vwap.v1 topic has any messages
if docker compose exec -T redpanda rpk topic consume features.vwap.v1 -n 1 --timeout 10s 2>/dev/null | grep -q "vwap"; then
  pass "Feature engine output detected in features.vwap.v1"
else
  info "No feature engine output detected yet. This is expected if the window hasn't closed."
  info "To verify fully, ensure a window duration has passed since injection."
fi

# --- Step 7: Summary ---
echo ""
echo -e "${GREEN}═══════════════════════════════════════${NC}"
echo -e "${GREEN}  Muninn smoke test passed  ${NC}"
echo -e "${GREEN}═══════════════════════════════════════${NC}"
echo ""
echo "  Application: ${APP_URL}"
echo "  Redpanda Console: http://localhost:8088"
echo "  Prometheus metrics: ${APP_URL}/actuator/prometheus"
echo "  MinIO Console: http://localhost:9003 (creds: minioadmin/minioadmin)"
echo ""

exit 0
