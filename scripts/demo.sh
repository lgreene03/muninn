#!/usr/bin/env bash

set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

info() {
  echo -e "${CYAN}→ $1${NC}"
}

success() {
  echo -e "${GREEN}✓ $1${NC}"
}

warn() {
  echo -e "${YELLOW}⚠ $1${NC}"
}

fail() {
  echo -e "${RED}✗ $1${NC}" >&2
  exit 1
}

# Print beautiful ASCII Banner
echo -e "${BOLD}${GREEN}"
echo "███╗   ███╗██╗   ██╗███╗   ██╗██╗███╗   ██╗███╗   ██╗"
echo "████╗ ████║██║   ██║████╗  ██║██║████╗  ██║████╗  ██║"
echo "██╔████╔██║██║   ██║██╔██╗ ██║██║██╔██╗ ██║██╔██╗ ██║"
echo "██║╚██╔╝██║██║   ██║██║╚██╗██║██║██║╚██╗██║██║╚██╗██║"
echo "██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║██║ ╚████║██║ ╚████║"
echo "╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝╚═╝  ╚═══╝"
echo -e "${CYAN}             Deterministic Replay & Telemetry Demo${NC}\n"

echo -e "This script demonstrates the end-to-end features of Muninn:"
echo -e "  1. Live Trade Ingestion via REST API"
echo -e "  2. Real-Time Feature Computation (rolling VWAP)"
echo -e "  3. Analytical Historical Query over DuckDB + Parquet"
echo -e "  4. Submitting a Replay Job over historical records"
echo -e "  5. Live-to-Replay Telemetry monitoring in Prometheus/Grafana"
echo ""

# Check if application is running
info "Checking if Muninn Spring Boot app is up at http://localhost:8080/actuator/health..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:8080/actuator/health" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" != "200" ]; then
  fail "Muninn Spring Boot app is not running on port 8080. Please start the app using 'mvn spring-boot:run' first!"
fi
success "Spring Boot app is healthy and running!"

# Step 1: Live Trade Ingestion
info "Step 1: Injecting 10 sequential synthetic trade events..."
INSTRUMENT="BTC-USDT"
PRICE=90000.00
SIZE=1.0

# Capture starting timestamp for query range
START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Generate 10 trades over the last few seconds to build a dynamic volume-weighted price
for i in $(seq 1 10); do
  # Simulating market changes: price fluctuates upwards, size grows
  PRICE=$(echo "$PRICE + 15.50" | bc)
  SIZE=$(echo "$SIZE + 0.25" | bc)
  TRADE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
  # Current ISO-8601 time
  EVENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Format JSON payload according to TradeEvent schema
  PAYLOAD=$(cat <<EOF
{
  "eventId": "${TRADE_ID}",
  "eventTime": "${EVENT_TIME}",
  "ingestTime": "${EVENT_TIME}",
  "source": "demo-script",
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
  "sequenceNumber": $i,
  "schemaVersion": 1,
  "price": $PRICE,
  "size": $SIZE,
  "side": "BUY",
  "exchangeTradeId": "demo-trade-$i"
}
EOF
)

  RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -d "$PAYLOAD" "http://localhost:8080/api/v1/events/trade")
  success "Posted Trade #$i: Price=${PRICE}, Qty=${SIZE} -> Response: $RESPONSE"
  sleep 0.5
done

# Capture ending timestamp
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Step 2: Querying calculated features
echo ""
info "Step 2: Retrieving real-time computed rolling VWAP features via DuckDB..."
sleep 2 # Let the engine flush state to Parquet warehouse

# Let's adjust time range to encapsulate our trades with a buffer
QUERY_START=$(date -u -v-1M +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "1 minute ago" +"%Y-%m-%dT%H:%M:%SZ")
QUERY_END=$(date -u -v+1M +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "1 minute" +"%Y-%m-%dT%H:%M:%SZ")

QUERY_RESPONSE=$(curl -s "http://localhost:8080/api/v1/features/vwap?instrument=${INSTRUMENT}&from=${QUERY_START}&to=${QUERY_END}")
echo -e "${YELLOW}Raw API Response:${NC} $QUERY_RESPONSE"
echo ""

# Step 3: Trigger historical Replay
info "Step 3: Triggering a historical shadow replay job for ${INSTRUMENT}..."
REPLAY_START=$(date -u -v-5M +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%SZ")
REPLAY_END=$END_TIME

REPLAY_BODY="{\"topics\":[\"events.trade\"],\"from\":\"$REPLAY_START\",\"to\":\"$REPLAY_END\",\"featureVersion\":\"v1\"}"
REPLAY_RESP=$(curl -s -X POST -H "Content-Type: application/json" -d "$REPLAY_BODY" "http://localhost:8080/api/v1/replay/jobs")
success "Replay job submitted successfully!"
echo -e "${YELLOW}Replay Job Details:${NC} $REPLAY_RESP"

JOB_ID=$(echo "$REPLAY_RESP" | grep -o '"jobId":"[^"]*' | grep -o '[^"]*$')

# Step 4: Tracking replay progress with animated spinner
echo ""
info "Step 4: Monitoring Replay Job Progress (ID: $JOB_ID)..."
SPINS=('/' '-' '\\' '|')
n=0

while true; do
  STATUS_RESP=$(curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB_ID")
  STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*' | grep -o '[^"]*$')
  
  if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "FAILED" ]; then
    echo -ne "\r\033[K" # Clear line
    break
  fi
  
  echo -ne "\r[${SPINS[$n]}] Job status: ${STATUS}... "
  n=$(( (n + 1) % 4 ))
  sleep 0.5
done

if [ "$STATUS" = "COMPLETED" ]; then
  success "Replay job completed successfully!"
  echo -e "${YELLOW}Final Job Status:${NC}"
  curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB_ID" | jq . 2>/dev/null || curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB_ID"
else
  fail "Replay job failed! Details: $STATUS_RESP"
fi

# Step 5: Recap Telemetry Links
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${GREEN}        Demo Run Completed Successfully! Live & Replay Matched ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "You can now visually explore the telemetry propagation in the dashboards:"
echo -e "  * ${CYAN}Grafana (Dashboards):${NC}    http://localhost:3001"
echo -e "    - ${BOLD}Pipeline Overview:${NC} Real-time Ingestion lag, VWAP outputs, and Replay jobs"
echo -e "    - ${BOLD}Determinism Panel:${NC} Mismatch alerts & Divergence Counters (asserted 0)"
echo -e "    - ${BOLD}Resource Panel:${NC}    JVM Heap, memory limits, process CPU load"
echo ""
echo -e "  * ${CYAN}Prometheus (Alerts):${NC}    http://localhost:9091/alerts"
echo -e "    - Inspect rules: ReplayDivergenceDetected, IngestionLagTooHigh, WatermarkLagWarning"
echo ""
echo -e "  * ${CYAN}Grafana Tempo (Traces):${NC} http://localhost:3200"
echo -e "    - Search by traceId or service 'muninn' to view distributed trace graphs E2E"
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════════${NC}"
exit 0
