#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NATS_BIN="$(cd "$SCRIPT_DIR/.." && node -p "require('@camera.ui/nats').natsServerPath()" 2>/dev/null)"

export PYTHONUNBUFFERED=1

NATS_PID=""

if [ ! -x "$NATS_BIN" ]; then
  echo -e "${RED}NATS binary not found at $NATS_BIN${NC}"
  echo -e "${YELLOW}Run 'npm install' in the repo root to install @camera.ui/nats.${NC}"
  exit 1
fi

cleanup() {
  echo -e "\n${YELLOW}Shutting down...${NC}"

  if [ -n "$NATS_PID" ] && kill -0 "$NATS_PID" 2>/dev/null; then
    kill -TERM "$NATS_PID" 2>/dev/null
    sleep 0.5
    kill -0 "$NATS_PID" 2>/dev/null && kill -9 "$NATS_PID" 2>/dev/null
    echo -e "${GREEN}NATS server stopped (PID: $NATS_PID)${NC}"
  fi

  echo -e "${GREEN}All processes terminated${NC}"
  exit 0
}

trap cleanup EXIT INT TERM

LOG_TO_FILE=false
if [[ "$1" == "--log" ]]; then
  LOG_TO_FILE=true
  echo "Logging enabled - output will be saved to .log files"
  echo ""

  mkdir -p logs
fi

if curl -s http://127.0.0.1:8222/healthz | grep -q '"status":"ok"'; then
  echo -e "${GREEN}NATS server is already running. Proceeding with tests...${NC}"
  echo ""
else
  echo -e "${YELLOW}Starting NATS server...${NC}"
  "$NATS_BIN" -c "$SCRIPT_DIR/nats.conf" &
  NATS_PID=$!

  NATS_READY=false
  for i in $(seq 1 30); do
    if curl -s http://127.0.0.1:8222/healthz >/dev/null 2>&1; then
      NATS_READY=true
      break
    fi
    sleep 1
  done

  if [ "$NATS_READY" = false ]; then
    echo -e "${RED}NATS server failed to start within 30s${NC}"
    exit 1
  fi

  echo -e "${GREEN}NATS is running (PID: $NATS_PID)${NC}"
  echo ""
fi

echo "Running all tests and collecting performance data..."
echo ""

tests=(
  "all-in-one-performance"
  "auto-chunking"
  "channel-communication"
  "channel-native-request"
  "concurrent"
  "generator-types"
  "isolated-connections"
  "isolated-handler"
  "isolated-service-server"
  "isolated-service"
  "large-data-transfer"
  "multi-service"
  "native-request-reply"
  "plain-object-properties"
  "private-channel-2"
  "private-channel"
  "property-decorator"
  "pull-vs-push-generators"
  "service-chunking"
  "service-pull-vs-push-generators"
  "service"
  "unified-streaming"
  "callback-subscription"
  "pull-callback-basic"
  "pull-callback-backpressure"
  "pull-callback-cancellation"
  "perf-hotpath"
)

python_supports_test() {
  local test_underscore="${1//-/_}"
  grep -qE "(elif|if) example_name == \"${test_underscore}\":" "$SCRIPT_DIR/../python/run_example.py" 2>/dev/null ||
    [[ -f "$SCRIPT_DIR/../python/examples/${test_underscore}.py" ]]
}

for test in "${tests[@]}"; do
  if [[ "$LOG_TO_FILE" == true ]]; then
    LOG_FILE="logs/${test}.log"

    > "$LOG_FILE"

    echo "=== Test: $test === (logging to $LOG_FILE)"

    echo "=== Test: $test ===" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"

    if python_supports_test "$test"; then
      echo "=== Python ===" >> "$LOG_FILE"
      echo "---" >> "$LOG_FILE"
      cd "$SCRIPT_DIR/../python" && python run_example.py ${test//-/_} >> "$SCRIPT_DIR/$LOG_FILE" 2>&1
      cd "$SCRIPT_DIR"
      echo "" >> "$LOG_FILE"
    else
      echo "=== Python (skipped — no example) ===" >> "$LOG_FILE"
      echo "" >> "$LOG_FILE"
    fi

    if [[ -f "$SCRIPT_DIR/../node/examples/${test}.ts" ]]; then
      echo "=== Node.js ===" >> "$LOG_FILE"
      echo "---" >> "$LOG_FILE"
      cd "$SCRIPT_DIR/../node" && tsx examples/${test}.ts >> "$SCRIPT_DIR/$LOG_FILE" 2>&1
      cd "$SCRIPT_DIR"
      echo "" >> "$LOG_FILE"
    else
      echo "=== Node.js (skipped — no example) ===" >> "$LOG_FILE"
      echo "" >> "$LOG_FILE"
    fi

    if [[ -f "$SCRIPT_DIR/../go/examples/${test}/main.go" ]]; then
      echo "=== Go ===" >> "$LOG_FILE"
      echo "---" >> "$LOG_FILE"
      cd "$SCRIPT_DIR/../go" && go run ./examples/${test}/ >> "$SCRIPT_DIR/$LOG_FILE" 2>&1
      cd "$SCRIPT_DIR"
      echo "" >> "$LOG_FILE"
    else
      echo "=== Go (skipped — no example) ===" >> "$LOG_FILE"
      echo "" >> "$LOG_FILE"
    fi

  else
    echo "=== Test: $test ==="
    echo ""

    if python_supports_test "$test"; then
      echo "=== Python ==="
      echo "---"
      cd "$SCRIPT_DIR/../python" && python run_example.py ${test//-/_} 2>&1
      cd "$SCRIPT_DIR"
      echo ""
    else
      echo "=== Python (skipped — no example) ==="
      echo ""
    fi

    if [[ -f "$SCRIPT_DIR/../node/examples/${test}.ts" ]]; then
      echo "=== Node.js ==="
      echo "---"
      cd "$SCRIPT_DIR/../node" && tsx examples/${test}.ts 2>&1
      cd "$SCRIPT_DIR"
      echo ""
    else
      echo "=== Node.js (skipped — no example) ==="
      echo ""
    fi

    if [[ -f "$SCRIPT_DIR/../go/examples/${test}/main.go" ]]; then
      echo "=== Go ==="
      echo "---"
      cd "$SCRIPT_DIR/../go" && go run ./examples/${test}/ 2>&1
      cd "$SCRIPT_DIR"
      echo ""
    else
      echo "=== Go (skipped — no example) ==="
      echo ""
    fi
  fi
done

if [[ "$LOG_TO_FILE" == true ]]; then
  echo ""
  echo "All tests completed. Log files saved in the logs directory."
fi
