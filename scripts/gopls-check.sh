#!/usr/bin/env bash
set -euo pipefail

# gopls check exits 0 whatever it finds, and it repeats a file for other
# platforms when build tags differ. Keep the findings for this machine and fail
# on any.
cd "$(dirname "$0")/../go"

if ! command -v gopls >/dev/null; then
  echo "gopls not found: go install golang.org/x/tools/gopls@latest" >&2
  exit 1
fi

findings=$(find . -name '*.go' -print0 | xargs -0 gopls check 2>&1 | grep -Ev '\[[a-z0-9]+,[a-z0-9]+\]$' || true)
if [ -n "$findings" ]; then
  echo "$findings" | sed "s|$PWD/||"
  exit 1
fi
