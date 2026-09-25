#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# List of all Go modules in the project
MODULES=(
    "."
    "./transports/redis"
    "./transports/sqlite"
)

cmd_setup() {
    log_info "Setting up development environment..."

    if ! command -v go &> /dev/null; then
        log_error "Go is not installed or not in PATH"
        exit 1
    fi

    go version

    # Always regenerate go.work to handle module list changes
    log_info "Generating Go workspace..."
    rm -f go.work go.work.sum
    go work init . ./transports/redis ./transports/sqlite

    log_info "Syncing workspace dependencies..."
    go work sync

    log_info "Running go mod tidy in all modules..."
    for mod in "${MODULES[@]}"; do
        log_info "  -> tidying $mod"
        (cd "$mod" && go mod tidy)
    done

    log_info "Setup complete."
}

cmd_test() {
    log_info "Running tests in all modules..."

    for mod in "${MODULES[@]}"; do
        log_info "  -> testing $mod"
        (cd "$mod" && go test -v -race -count=1 ./...)
    done

    log_info "All tests passed."
}

cmd_test_short() {
    log_info "Running tests (short mode) in all modules..."

    for mod in "${MODULES[@]}"; do
        log_info "  -> testing $mod"
        (cd "$mod" && go test -short -count=1 ./...)
    done

    log_info "All short tests passed."
}

cmd_tidy() {
    log_info "Running go mod tidy in all modules..."

    for mod in "${MODULES[@]}"; do
        log_info "  -> tidying $mod"
        (cd "$mod" && go mod tidy)
    done

    go work sync
    log_info "Tidy complete."
}

cmd_build() {
    log_info "Building all packages..."

    for mod in "${MODULES[@]}"; do
        log_info "  -> building $mod"
        (cd "$mod" && go build ./...)
    done

    log_info "Build complete."
}

cmd_vet() {
    log_info "Running go vet in all modules..."

    for mod in "${MODULES[@]}"; do
        log_info "  -> vetting $mod"
        (cd "$mod" && go vet ./...)
    done

    log_info "Vet complete."
}

cmd_clean() {
    log_info "Cleaning up..."

    for mod in "${MODULES[@]}"; do
        (cd "$mod" && go clean -cache -testcache)
    done

    # Remove workspace files
    rm -f go.work go.work.sum

    log_info "Clean complete."
}

cmd_test_docker() {
    log_info "Running tests in Docker (Redis + all modules)..."
    docker compose up --abort-on-container-exit
    docker compose down
    log_info "Docker tests complete."
}

cmd_test_redis_only() {
    log_info "Running Redis transport tests in Docker..."
    docker compose run --rm test sh -c \
        "cd /app/transports/redis && go mod download && go test -v -race ./..."
    docker compose down
    log_info "Redis-only tests complete."
}

cmd_ci() {
    log_info "Running CI pipeline locally..."
    cmd_vet
    cmd_build
    cmd_test
    log_info "CI pipeline passed."
}

cmd_upgrade() {
    local mode="${1:-minor}"
    local get_flag="-u"

    if [[ "$mode" == "patch" ]]; then
        get_flag="-u=patch"
        log_info "Upgrading dependencies (patch only) in all modules..."
    else
        log_info "Upgrading dependencies (direct + indirect) in all modules..."
    fi

    for mod in "${MODULES[@]}"; do
        log_info "  -> upgrading $mod"
        (cd "$mod" && go get "$get_flag" ./... && go mod tidy -go=1.27)
    done

    go work sync
    log_info "Upgrade complete. Run './dev.sh ci' to verify."
}

cmd_release() {
    local version=""
    local push=false

    for arg in "$@"; do
        case "$arg" in
            --push) push=true ;;
            *)      version="$arg" ;;
        esac
    done

    if [[ -z "$version" ]]; then
        log_error "Usage: ./dev.sh release <version> [--push]"
        exit 1
    fi

    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        log_error "Invalid version '$version' (expected semver like 1.0.14)"
        exit 1
    fi

    # Guard: clean working tree (staged, unstaged, untracked)
    if [[ -n "$(git status --porcelain)" ]]; then
        log_error "Working tree has uncommitted changes. Commit or stash first."
        exit 1
    fi

    # Guard: no replace directives in the root module — they break downstream
    # consumers. Submodule ../../ replaces are dev-only and ignored by consumers.
    local replaces
    replaces=$(grep -l '^replace ' go.mod 2>/dev/null || true)
    if [[ -n "$replaces" ]]; then
        log_error "replace directives found, remove before release:"
        echo "$replaces"
        exit 1
    fi

    # Build the tag list: root -> vX.Y.Z, submodule -> <path>/vX.Y.Z
    local tags=()
    for mod in "${MODULES[@]}"; do
        local prefix="${mod#./}"
        if [[ "$prefix" == "." ]]; then
            tags+=("v$version")
        else
            tags+=("$prefix/v$version")
        fi
    done

    # Guard: none of the tags may already exist (local or remote)
    local remote_tags=""
    remote_tags=$(git ls-remote --tags origin 2>/dev/null | awk '{print $2}' | sed 's|refs/tags/||' || true)

    local existing=()
    for tag in "${tags[@]}"; do
        if git rev-parse -q --verify "refs/tags/$tag" >/dev/null 2>&1; then
            existing+=("$tag (local)")
        elif grep -qx "$tag" <<<"$remote_tags"; then
            existing+=("$tag (remote)")
        fi
    done

    if [[ ${#existing[@]} -gt 0 ]]; then
        log_error "These tags already exist:"
        printf '  %s\n' "${existing[@]}"
        exit 1
    fi

    log_info "Release v$version will create tags:"
    printf '  %s\n' "${tags[@]}"

    if ! $push; then
        log_warn "Dry run — no tags created. Re-run with --push to create and push."
        exit 0
    fi

    for tag in "${tags[@]}"; do
        git tag "$tag"
        log_info "  tagged $tag"
    done

    git push origin "${tags[@]}"
    log_info "Release v$version pushed."
}

cmd_help() {
    cat <<EOF
Usage: ./dev.sh <command>

Commands:
  setup       Initialize Go workspace, sync deps, tidy all modules
  test        Run all tests with -race in all modules
  test-short  Run tests with -short flag (skips integration tests)
  test-docker Run all tests via Docker Compose (includes Redis)
  test-redis-only Run only Redis transport tests via Docker Compose
  tidy        Run 'go mod tidy' in all modules and sync workspace
  build       Build all packages in all modules
  vet         Run 'go vet' in all modules
  clean       Clean Go caches and remove generated files
  ci          Run vet + build + test (full CI simulation)
  upgrade     Upgrade all dependencies (direct + indirect) to latest
  upgrade-patch Upgrade dependencies to latest patch versions only
  release <v> [--push]  Tag all modules as v<v> (dry run without --push)
  help        Show this help message

Modules managed:
  .                          (root module: core + fake transport)
  ./transports/redis         (Redis transport submodule)
  ./transports/sqlite        (SQLite transport submodule)
EOF
}

main() {
    if [[ $# -eq 0 ]]; then
        cmd_help
        exit 1
    fi

    case "$1" in
        setup)            cmd_setup ;;
        test)             cmd_test ;;
        test-short)       cmd_test_short ;;
        test-docker)      cmd_test_docker ;;
        test-redis-only)  cmd_test_redis_only ;;
        tidy)             cmd_tidy ;;
        build)            cmd_build ;;
        vet)              cmd_vet ;;
        clean)            cmd_clean ;;
        ci)               cmd_ci ;;
        upgrade)          cmd_upgrade minor ;;
        upgrade-patch)    cmd_upgrade patch ;;
        release)          shift; cmd_release "$@" ;;
        help|--help|-h)   cmd_help ;;
        *)
            log_error "Unknown command: $1"
            cmd_help
            exit 1
            ;;
    esac
}

main "$@"
