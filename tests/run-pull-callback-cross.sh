#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NATS_BIN="$(cd "$SCRIPT_DIR/.." && node -p "require('@camera.ui/nats').natsServerPath()" 2>/dev/null)"

export PYTHONUNBUFFERED=1

NATS_PID=""
SERVER_PIDS=()

if [ ! -x "$NATS_BIN" ]; then
  echo -e "${RED}NATS binary not found at $NATS_BIN${NC}"
  echo -e "${YELLOW}Run 'npm install' in the repo root to install @camera.ui/nats.${NC}"
  exit 1
fi

NATS_STARTED_BY_US=false

cleanup() {
  echo -e "\n${YELLOW}Cleaning up...${NC}"

  for pid in "${SERVER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null
    fi
  done
  sleep 1
  for pid in "${SERVER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null
    fi
  done

  pkill -9 -f "pull-callback-cross --role server" 2>/dev/null
  pkill -9 -f "pull_callback_cross --role server" 2>/dev/null

  if [[ "$NATS_STARTED_BY_US" == true && -n "$NATS_PID" ]] && kill -0 "$NATS_PID" 2>/dev/null; then
    kill -TERM "$NATS_PID" 2>/dev/null
    sleep 0.5
    kill -0 "$NATS_PID" 2>/dev/null && kill -9 "$NATS_PID" 2>/dev/null
    echo -e "${GREEN}NATS stopped${NC}"
  fi
}

trap cleanup EXIT INT TERM

if curl -s http://127.0.0.1:8222/healthz 2>/dev/null | grep -q '"status":"ok"'; then
  echo -e "${GREEN}NATS server already running${NC}"
else
  echo -e "${YELLOW}Starting NATS server...${NC}"
  "$NATS_BIN" -c "$SCRIPT_DIR/nats.conf" &
  NATS_PID=$!
  NATS_STARTED_BY_US=true

  NATS_READY=false
  for _ in $(seq 1 30); do
    if curl -s http://127.0.0.1:8222/healthz >/dev/null 2>&1; then
      NATS_READY=true
      break
    fi
    sleep 1
  done

  if [[ "$NATS_READY" == false ]]; then
    echo -e "${RED}NATS failed to start within 30s${NC}"
    exit 2
  fi
  echo -e "${GREEN}NATS running (PID $NATS_PID)${NC}"
fi
echo ""

echo -e "${CYAN}Starting servers${NC}"

cd "$SCRIPT_DIR/../node"
tsx examples/pull-callback-cross.ts --role server --name node >/tmp/pullcb-cross-node-server.log 2>&1 &
NODE_SRV_PID=$!
SERVER_PIDS+=("$NODE_SRV_PID")
echo -e "${GREEN}Node server PID $NODE_SRV_PID${NC}"

cd "$SCRIPT_DIR/../go"
go run ./examples/pull-callback-cross/ --role server --name go >/tmp/pullcb-cross-go-server.log 2>&1 &
GO_SRV_PID=$!
SERVER_PIDS+=("$GO_SRV_PID")
echo -e "${GREEN}Go server PID $GO_SRV_PID${NC}"

cd "$SCRIPT_DIR/../python"
python run_example.py pull_callback_cross --role server --name python >/tmp/pullcb-cross-py-server.log 2>&1 &
PY_SRV_PID=$!
SERVER_PIDS+=("$PY_SRV_PID")
echo -e "${GREEN}Python server PID $PY_SRV_PID${NC}"

echo -e "${YELLOW}Waiting 5s for servers to register...${NC}"
sleep 5
echo ""

TARGETS="node,go,python"
FAILED=0

echo -e "${CYAN}Client: Node${NC}"
cd "$SCRIPT_DIR/../node"
if ! tsx examples/pull-callback-cross.ts --role client --targets "$TARGETS"; then
  FAILED=$((FAILED + 1))
fi
echo ""

echo -e "${CYAN}Client: Go${NC}"
cd "$SCRIPT_DIR/../go"
if ! go run ./examples/pull-callback-cross/ --role client --targets "$TARGETS"; then
  FAILED=$((FAILED + 1))
fi
echo ""

echo -e "${CYAN}Client: Python${NC}"
cd "$SCRIPT_DIR/../python"
if ! python run_example.py pull_callback_cross --role client --targets "$TARGETS"; then
  FAILED=$((FAILED + 1))
fi
echo ""

if [[ $FAILED -eq 0 ]]; then
  echo -e "${GREEN}ALL 9 PAIRS PASSED${NC}"
  exit 0
else
  echo -e "${RED}$FAILED client(s) had failures${NC}"
  echo -e "${YELLOW}Server logs at /tmp/pullcb-cross-{node,go,py}-server.log${NC}"
  exit 1
fi
