#!/bin/sh

# disable go modules
export GOPATH=""

# disable cgo
export CGO_ENABLED=0

set -e
set -x

runner_version=$(git describe --tags --match 'v[0-9]*' --abbrev=7 2>/dev/null || printf 'devel')
runner_commit=$(git rev-parse HEAD)
runner_tree_state=clean
if ! git diff --quiet HEAD --; then
  runner_tree_state=dirty
fi
runner_version_pkg=github.com/drone-runners/drone-runner-kube/internal/version
runner_ldflags="-X $runner_version_pkg.version=$runner_version -X $runner_version_pkg.gitCommit=$runner_commit -X $runner_version_pkg.gitTreeState=$runner_tree_state"

# Keep the existing release architectures unless a caller selects a subset.
if [ "$#" -eq 0 ]; then
  set -- amd64 arm64 arm
fi
for runner_arch do
  GOOS=linux GOARCH="$runner_arch" go build -ldflags "$runner_ldflags" -o "release/linux/$runner_arch/drone-runner-kube"
done
