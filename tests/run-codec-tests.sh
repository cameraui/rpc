#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PYTHONUNBUFFERED=1

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
DIM='\033[2m'
NC='\033[0m'

TOTAL_PASSED=0
TOTAL_FAILED=0
PHASE_RESULTS=()

cleanup() {
    rm -f /tmp/py-encoded-*.msgpack
    rm -f /tmp/node-encoded-*.msgpack
    rm -f /tmp/go-encoded-*.msgpack
}

trap cleanup EXIT

run_phase() {
    local phase_name="$1"
    local command="$2"
    local exit_code=0

    echo ""
    echo -e "${CYAN}${phase_name}${NC}"
    echo ""

    eval "$command" || exit_code=$?

    if [ $exit_code -ne 0 ]; then
        PHASE_RESULTS+=("${RED}FAIL${NC} $phase_name")
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
    else
        PHASE_RESULTS+=("${GREEN}OK${NC}   $phase_name")
        TOTAL_PASSED=$((TOTAL_PASSED + 1))
    fi

    return 0  # Don't fail the whole script on individual phase failure
}

echo -e "${GREEN}Cross-Language MessagePack Codec Tests${NC}"
echo ""
echo -e "${DIM}Python: ormsgpack${NC}"
echo -e "${DIM}Node.js: msgpackr${NC}"
echo -e "${DIM}Go: vmihailenco/msgpack/v5${NC}"

echo ""
echo -e "${YELLOW}Cleaning up old test files...${NC}"
cleanup
echo -e "${GREEN}Done${NC}"

run_phase "Python encode + roundtrip" \
    "cd '$SCRIPT_DIR' && python test-codec.py"

run_phase "Node.js encode + roundtrip" \
    "cd '$SCRIPT_DIR' && tsx test-codec.ts"

run_phase "Go encode + roundtrip + struct decode" \
    "cd '$SCRIPT_DIR' && go run ./go-codec"

run_phase "Python cross-decode (Node.js + Go data)" \
    "cd '$SCRIPT_DIR' && python test-codec-cross.py"

run_phase "Node.js cross-decode (Python + Go data)" \
    "cd '$SCRIPT_DIR' && tsx test-codec-cross.ts"

run_phase "Go cross-decode (Python + Node.js data)" \
    "cd '$SCRIPT_DIR' && go run ./go-codec-cross"

echo ""
echo -e "${CYAN}Summary${NC}"
echo ""

for result in "${PHASE_RESULTS[@]}"; do
    echo -e "  $result"
done

echo ""
echo -e "  Phases: ${GREEN}${TOTAL_PASSED} passed${NC}, ${RED}${TOTAL_FAILED} failed${NC} (of $((TOTAL_PASSED + TOTAL_FAILED)) total)"
echo ""

if [ "$TOTAL_FAILED" -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
