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
PYTHON_PID=""
NODE_PID=""
GO_PID=""

if [ ! -x "$NATS_BIN" ]; then
    echo -e "${RED}NATS binary not found at $NATS_BIN${NC}"
    echo -e "${YELLOW}Run 'npm install' in the repo root to install @camera.ui/nats.${NC}"
    exit 1
fi

BIN_DIR="$(mktemp -d)"

cleanup() {
    local exit_code=$?
    echo -e "\n${YELLOW}Shutting down...${NC}"

    for pid_var in GO_PID NODE_PID PYTHON_PID; do
        pid="${!pid_var}"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null
            echo -e "${GREEN}Sent SIGTERM to $pid_var (PID: $pid)${NC}"
        fi
    done

    sleep 1

    pkill -9 -f "python.*cross_language" 2>/dev/null
    pkill -9 -f "tsx.*cross-language" 2>/dev/null
    pkill -9 -f "node.*cross-language" 2>/dev/null
    pkill -9 -f "run_example.py cross_language" 2>/dev/null
    pkill -9 -f "examples/cross-language" 2>/dev/null

    if [ -n "$NATS_PID" ] && kill -0 "$NATS_PID" 2>/dev/null; then
        kill -TERM "$NATS_PID" 2>/dev/null
        sleep 0.5
        kill -0 "$NATS_PID" 2>/dev/null && kill -9 "$NATS_PID" 2>/dev/null
        echo -e "${GREEN}NATS server stopped (PID: $NATS_PID)${NC}"
    fi

    rm -rf "$BIN_DIR" 2>/dev/null
    echo -e "${GREEN}All processes terminated${NC}"
    exit "$exit_code"
}

trap cleanup EXIT INT TERM

MODE="${1:-all}"

case "$MODE" in
    node-python) MODE="python-node" ;;
    python-go)   MODE="go-python" ;;
    node-go)     MODE="go-node" ;;
esac

case "$MODE" in
    all|python-node|go-python|go-node) ;;
    *)
        echo -e "${RED}Unknown mode: $MODE${NC}"
        echo ""
        echo "Usage: $0 [mode]"
        echo ""
        echo "Modes:"
        echo "  all          Python + Node + Go (default)"
        echo "  python-node  Python and Node"
        echo "  go-python    Go and Python"
        echo "  go-node      Go and Node"
        exit 1
        ;;
esac

PYTHON_TARGETS=""
NODE_TARGETS=""
GO_TARGETS=""

case "$MODE" in
    all)
        PYTHON_TARGETS="node-service,go-service"
        NODE_TARGETS="python-service,go-service"
        GO_TARGETS="python-service,node-service"
        ;;
    python-node)
        PYTHON_TARGETS="node-service"
        NODE_TARGETS="python-service"
        ;;
    go-python)
        PYTHON_TARGETS="go-service"
        GO_TARGETS="python-service"
        ;;
    go-node)
        NODE_TARGETS="go-service"
        GO_TARGETS="node-service"
        ;;
esac

echo -e "${GREEN}Cross-Language RPC Test${NC}"
echo -e "${CYAN}   Mode: $MODE${NC}"
[ -n "$PYTHON_TARGETS" ] && echo -e "${CYAN}   Python targets: $PYTHON_TARGETS${NC}"
[ -n "$NODE_TARGETS" ]   && echo -e "${CYAN}   Node targets:   $NODE_TARGETS${NC}"
[ -n "$GO_TARGETS" ]     && echo -e "${CYAN}   Go targets:     $GO_TARGETS${NC}"
echo ""

echo -e "${YELLOW}Cleaning up existing processes...${NC}"
pkill -f "cross_language" 2>/dev/null
pkill -f "cross-language" 2>/dev/null
sleep 1

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

echo -e "${GREEN}NATS is running (PID: $NATS_PID)${NC}\n"

# Pre-build the Go example so all three servers can start together. `go run`
# neither forwards SIGTERM nor propagates the program's exit code, so we run the
# compiled binary directly.
if [ -n "$GO_TARGETS" ]; then
    echo -e "${GREEN}Building Go example...${NC}"
    if ! (cd "$SCRIPT_DIR/../go" && go build -o "$BIN_DIR/cross-go" ./examples/cross-language/); then
        echo -e "${RED}Go build failed${NC}"
        exit 1
    fi
fi

if [ -n "$PYTHON_TARGETS" ]; then
    echo -e "${GREEN}Starting Python server...${NC}"
    # exec so $! is the python process itself (not the cd-subshell), otherwise
    # our SIGTERM would hit the subshell and the example never shuts down cleanly.
    ( cd "$SCRIPT_DIR/../python" && exec python run_example.py cross_language_test --targets "$PYTHON_TARGETS" ) > "$BIN_DIR/python.log" 2>&1 &
    PYTHON_PID=$!
fi

if [ -n "$NODE_TARGETS" ]; then
    echo -e "${GREEN}Starting Node.js server...${NC}"
    ( cd "$SCRIPT_DIR/../node" && exec tsx examples/cross-language.ts --targets "$NODE_TARGETS" ) > "$BIN_DIR/node.log" 2>&1 &
    NODE_PID=$!
fi

if [ -n "$GO_TARGETS" ]; then
    echo -e "${GREEN}Starting Go server...${NC}"
    "$BIN_DIR/cross-go" --targets "$GO_TARGETS" > "$BIN_DIR/go.log" 2>&1 &
    GO_PID=$!
fi

# Wait until every started runtime has finished its client phase (each prints
# "server running" once done) or has exited early. Tearing the processes down
# after a fixed delay would cut calls off mid-flight.
deadline=$((SECONDS + 120))
while true; do
    pending=0
    for pair in "PYTHON_PID:python" "NODE_PID:node" "GO_PID:go"; do
        pid_var="${pair%%:*}"
        name="${pair##*:}"
        pid="${!pid_var}"
        [ -z "$pid" ] && continue
        grep -q "server running" "$BIN_DIR/$name.log" 2>/dev/null && continue
        kill -0 "$pid" 2>/dev/null || continue
        pending=$((pending + 1))
    done
    [ "$pending" -eq 0 ] && break
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo -e "${YELLOW}Timed out waiting for client phases to complete${NC}"
        break
    fi
    sleep 0.5
done

echo -e "\n${CYAN}Stopping servers & collecting results${NC}"

# Ask each runtime to shut down. Each example tracks its own failed calls and
# exits non-zero if any failed, so we just collect the exit codes.
for pid_var in PYTHON_PID NODE_PID GO_PID; do
    pid="${!pid_var}"
    [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null
done

RESULT=0
for pair in "PYTHON_PID:Python" "NODE_PID:Node.js" "GO_PID:Go"; do
    pid_var="${pair%%:*}"
    label="${pair##*:}"
    pid="${!pid_var}"
    [ -z "$pid" ] && continue
    if wait "$pid"; then
        echo -e "${GREEN}${label} passed${NC}"
    else
        echo -e "${RED}${label} reported failures (exit $?)${NC}"
        RESULT=1
    fi
done

if [ "$RESULT" -eq 0 ]; then
    echo -e "\n${GREEN}Cross-language RPC test passed (mode: $MODE)${NC}"
else
    echo -e "\n${RED}Cross-language RPC test FAILED (mode: $MODE)${NC}"
    for name in python node go; do
        if [ -f "$BIN_DIR/$name.log" ]; then
            echo -e "\n${YELLOW}----- $name output -----${NC}"
            cat "$BIN_DIR/$name.log"
        fi
    done
fi

exit "$RESULT"
