#!/bin/sh

set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/bin" "$fixture/repo/scripts"
cp "$repo_root/scripts/build.sh" "$fixture/repo/scripts/build.sh"
cat >"$fixture/bin/go" <<'EOF'
#!/bin/sh
printf '%s\n' "$GOOS|$GOARCH|$*" >"$GO_CAPTURE"
EOF
chmod +x "$fixture/bin/go"

cd "$fixture/repo"
git init -q
git config user.name test
git config user.email test@example.com
git config commit.gpgsign false
git config tag.gpgsign false
git config core.hooksPath /dev/null
printf 'initial\n' >tracked
git add tracked scripts/build.sh
git commit -qm initial

run_build() {
  GO_CAPTURE="$fixture/capture" PATH="$fixture/bin:$PATH" sh scripts/build.sh arm64 >/dev/null 2>&1
  cat "$fixture/capture"
}

assert_contains() {
  case "$1" in
    *"$2"*) ;;
    *) printf 'expected %s to contain %s\n' "$1" "$2" >&2; exit 1 ;;
  esac
}

commit=$(git rev-parse HEAD)
output=$(run_build)
assert_contains "$output" "linux|arm64|build"
assert_contains "$output" "-X github.com/drone-runners/drone-runner-kube/internal/version.version=devel"
assert_contains "$output" "-X github.com/drone-runners/drone-runner-kube/internal/version.gitCommit=$commit"
assert_contains "$output" "-X github.com/drone-runners/drone-runner-kube/internal/version.gitTreeState=clean"

git tag v1.2.3
output=$(run_build)
assert_contains "$output" "version=v1.2.3 -X"

printf 'next\n' >>tracked
git commit -qam next
commit=$(git rev-parse HEAD)
short_commit=$(git rev-parse --short=7 HEAD)
output=$(run_build)
assert_contains "$output" "version=v1.2.3-1-g$short_commit -X"
assert_contains "$output" "gitCommit=$commit"

printf 'dirty\n' >>tracked
output=$(run_build)
assert_contains "$output" "gitTreeState=dirty"
