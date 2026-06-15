#!/bin/bash
set -euo pipefail

# Bump a package's version, commit it, tag it and push. The matching GitHub
# Actions release workflow then runs the full test suite and publishes.
#
#   node    -> bumps node/package.json,      tag node-vX.Y.Z   (npm)
#   python  -> bumps python/pyproject.toml,   tag python-vX.Y.Z (PyPI)
#   go      -> no version file, tag go/vX.Y.Z only              (Go module)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

usage() {
  cat <<EOF
Usage: scripts/release.sh <node|python|go> <version|major|minor|patch> [--yes] [--skip-checks]

Examples:
  scripts/release.sh node 1.0.2
  scripts/release.sh python patch
  scripts/release.sh go minor

Options:
  --yes, -y       Push without the confirmation prompt.
  --skip-checks   Skip the local unit-test pre-flight (the workflow still runs it).
EOF
  exit 1
}

PKG="${1:-}"
SPEC="${2:-}"
YES=false
SKIP_CHECKS=false
for arg in "${@:3}"; do
  case "$arg" in
    --yes | -y) YES=true ;;
    --skip-checks) SKIP_CHECKS=true ;;
    *)
      echo "Unknown option: $arg"
      usage
      ;;
  esac
done

[ -z "$PKG" ] || [ -z "$SPEC" ] && usage
case "$PKG" in node | python | go) ;; *) usage ;; esac

cd "$ROOT"

# Safety: clean tree, on main, not behind origin.
if [ -n "$(git status --porcelain)" ]; then
  echo -e "${RED}Working tree not clean - commit or stash first.${NC}"
  exit 1
fi
branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$branch" != "main" ]; then
  echo -e "${RED}Not on main (on '$branch').${NC}"
  exit 1
fi
git fetch -q origin main || true
if [ -n "$(git rev-list HEAD..origin/main 2>/dev/null)" ]; then
  echo -e "${RED}Local main is behind origin/main - pull first.${NC}"
  exit 1
fi

case "$PKG" in
  node) cur="$(node -p "require('./node/package.json').version")" ;;
  python) cur="$(grep -E '^version' python/pyproject.toml | head -1 | sed -E 's/.*"(.*)".*/\1/')" ;;
  go)
    cur="$(git tag -l 'go/v*' | sed 's#go/v##' | sort -V | tail -1)"
    cur="${cur:-0.0.0}"
    ;;
esac

bump() {
  local IFS='.'
  read -r ma mi pa <<<"$1"
  case "$2" in
    major) echo "$((ma + 1)).0.0" ;;
    minor) echo "$ma.$((mi + 1)).0" ;;
    patch) echo "$ma.$mi.$((pa + 1))" ;;
  esac
}

case "$SPEC" in
  major | minor | patch) NEW="$(bump "$cur" "$SPEC")" ;;
  *) NEW="$SPEC" ;;
esac

if ! echo "$NEW" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo -e "${RED}Invalid version '$NEW' (expected X.Y.Z).${NC}"
  exit 1
fi

case "$PKG" in
  node) TAG="node-v$NEW" ;;
  python) TAG="python-v$NEW" ;;
  go) TAG="go/v$NEW" ;;
esac

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo -e "${RED}Tag $TAG already exists.${NC}"
  exit 1
fi

echo -e "${CYAN}Releasing $PKG: $cur -> $NEW (tag $TAG)${NC}"

if [ "$SKIP_CHECKS" = false ]; then
  echo -e "${YELLOW}Running $PKG unit tests...${NC}"
  npm run "test:unit:$PKG"
fi

committed=false
case "$PKG" in
  node)
    (cd node && npm version "$NEW" --no-git-tag-version >/dev/null)
    git add node/package.json node/package-lock.json
    ;;
  python)
    sed -i.bak -E "s/^version = \".*\"/version = \"$NEW\"/" python/pyproject.toml
    rm -f python/pyproject.toml.bak
    git add python/pyproject.toml
    ;;
  go) ;; # no file to change; tag points at current HEAD
esac

if ! git diff --cached --quiet; then
  git commit -q -m "release($PKG): v$NEW"
  committed=true
  echo -e "${GREEN}Committed version bump.${NC}"
fi

git tag "$TAG"
echo -e "${GREEN}Created tag $TAG.${NC}"

if [ "$YES" = false ]; then
  printf "Push main + %s and trigger the release? [y/N] " "$TAG"
  read -r ans
  case "$ans" in
    y | Y | yes) ;;
    *)
      git tag -d "$TAG" >/dev/null
      [ "$committed" = true ] && git reset -q --hard HEAD~1
      echo "Aborted - tag and any bump commit were undone locally."
      exit 0
      ;;
  esac
fi

git push -q origin main
git push -q origin "$TAG"
echo -e "${GREEN}Pushed. Watch the release workflow under the repo's Actions tab.${NC}"
