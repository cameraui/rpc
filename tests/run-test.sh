#!/bin/bash

# Run a single RPC example in a chosen language.
#
# Usage:
#   ./run-test.sh --lang <node|python|go> --test <test-name>
#
# Example:
#   ./run-test.sh --lang node   --test pull-callback-basic
#   ./run-test.sh --lang python --test concurrent
#   ./run-test.sh --lang go     --test unified-streaming

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NATS_BIN="$SCRIPT_DIR/../node_modules/@camera.ui/nats/binary/nats"

export PYTHONUNBUFFERED=1

LANG_ARG=""
TEST_ARG=""
NATS_PID=""
NATS_STARTED_BY_US=false

if [ ! -x "$NATS_BIN" ]; then
  echo -e "${RED}NATS binary not found at $NATS_BIN${NC}"
  echo -e "${YELLOW}Run 'npm install' in the repo root to install @camera.ui/nats.${NC}"
  exit 1
fi

usage() {
  cat <<EOF
Usage: $0 --lang <node|python|go> --test <test-name>

Examples:
  $0 --lang node   --test pull-callback-basic
  $0 --lang python --test concurrent
  $0 --lang go     --test unified-streaming
EOF
  exit 1
}

cleanup() {
  local exit_code=$?

  if [[ "$NATS_STARTED_BY_US" == true && -n "$NATS_PID" ]] && kill -0 "$NATS_PID" 2>/dev/null; then
    echo -e "\n${YELLOW}Stopping NATS (PID $NATS_PID)...${NC}"
    kill -TERM "$NATS_PID" 2>/dev/null
    sleep 0.5
    kill -0 "$NATS_PID" 2>/dev/null && kill -9 "$NATS_PID" 2>/dev/null
    echo -e "${GREEN}NATS stopped${NC}"
  fi

  exit $exit_code
}

trap cleanup EXIT INT TERM

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lang)
      LANG_ARG="$2"
      shift 2
      ;;
    --test)
      TEST_ARG="$2"
      shift 2
      ;;
    -h | --help)
      usage
      ;;
    *)
      echo -e "${RED}Unknown argument: $1${NC}"
      usage
      ;;
  esac
done

if [[ -z "$LANG_ARG" || -z "$TEST_ARG" ]]; then
  echo -e "${RED}Missing --lang or --test${NC}"
  usage
fi

if [[ "$LANG_ARG" != "node" && "$LANG_ARG" != "python" && "$LANG_ARG" != "go" ]]; then
  echo -e "${RED}Invalid --lang '$LANG_ARG' (must be node, python, or go)${NC}"
  usage
fi

TEST_UNDERSCORE="${TEST_ARG//-/_}"
EXAMPLE_LOCATION=""

case "$LANG_ARG" in
  node)
    EXAMPLE_LOCATION="$SCRIPT_DIR/../node/examples/${TEST_ARG}.ts"
    if [[ ! -f "$EXAMPLE_LOCATION" ]]; then
      echo -e "${RED}Node example not found: $EXAMPLE_LOCATION${NC}"
      exit 1
    fi
    ;;
  python)
    # Python routes via run_example.py; accept either an entry in that script
    # or a matching file in examples/.
    if grep -qE "(elif|if) example_name == \"${TEST_UNDERSCORE}\":" "$SCRIPT_DIR/../python/run_example.py" 2>/dev/null; then
      EXAMPLE_LOCATION="run_example.py:${TEST_UNDERSCORE}"
    elif [[ -f "$SCRIPT_DIR/../python/examples/${TEST_UNDERSCORE}.py" ]]; then
      EXAMPLE_LOCATION="$SCRIPT_DIR/../python/examples/${TEST_UNDERSCORE}.py"
    else
      echo -e "${RED}Python example not found: '${TEST_UNDERSCORE}' (neither in run_example.py nor examples/${TEST_UNDERSCORE}.py)${NC}"
      exit 1
    fi
    ;;
  go)
    EXAMPLE_LOCATION="$SCRIPT_DIR/../go/examples/${TEST_ARG}/main.go"
    if [[ ! -f "$EXAMPLE_LOCATION" ]]; then
      echo -e "${RED}Go example not found: $EXAMPLE_LOCATION${NC}"
      exit 1
    fi
    ;;
esac

echo -e "${GREEN}Found ${LANG_ARG} example: ${EXAMPLE_LOCATION}${NC}"

if curl -s http://127.0.0.1:8222/healthz 2>/dev/null | grep -q '"status":"ok"'; then
  echo -e "${GREEN}NATS server already running, leaving it up${NC}"
else
  echo -e "${YELLOW}Starting NATS server...${NC}"
  "$NATS_BIN" -c "$SCRIPT_DIR/nats.conf" &
  NATS_PID=$!
  NATS_STARTED_BY_US=true

  NATS_READY=false
  for i in $(seq 1 30); do
    if curl -s http://127.0.0.1:8222/healthz >/dev/null 2>&1; then
      NATS_READY=true
      break
    fi
    sleep 1
  done

  if [[ "$NATS_READY" == false ]]; then
    echo -e "${RED}NATS server failed to start within 30s${NC}"
    exit 1
  fi

  echo -e "${GREEN}NATS running (PID $NATS_PID)${NC}"
fi

echo ""
echo "Running: ${LANG_ARG} / ${TEST_ARG}"
echo ""

case "$LANG_ARG" in
  node)
    cd "$SCRIPT_DIR/../node" && tsx "examples/${TEST_ARG}.ts"
    ;;
  python)
    cd "$SCRIPT_DIR/../python" && python run_example.py "${TEST_UNDERSCORE}"
    ;;
  go)
    cd "$SCRIPT_DIR/../go" && go run "./examples/${TEST_ARG}/"
    ;;
esac
EXIT_CODE=$?

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
  echo -e "${GREEN}Test passed (${LANG_ARG}/${TEST_ARG})${NC}"
else
  echo -e "${RED}Test failed (${LANG_ARG}/${TEST_ARG}) with exit code $EXIT_CODE${NC}"
fi

exit $EXIT_CODE
