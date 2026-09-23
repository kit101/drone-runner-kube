# drone-runner-kube

The `kubernetes` runner executes pipelines inside Kubernetes pods. This runner is an alternative to the docker runner and is optimize for teams running Drone on Kubernetes. This requires Drone server `1.6.0` or higher.

<!--
Documentation:<br/>
https://kubernetes-runner.docs.drone.io
-->

Technical Support:<br/>
https://discourse.drone.io

Issue Tracker and Roadmap:<br/>
https://trello.com/b/ttae5E5o/drone

## GitHub Actions images

The [Docker image workflow](.github/workflows/docker.yml) tests the code and builds
`linux/amd64` and `linux/arm64` images using the existing Dockerfiles and the Go
version declared in `go.mod`.

To publish to Docker Hub, add these repository secrets under **Settings > Secrets
and variables > Actions**:

- `DOCKERHUB_USERNAME`: `kit101z` (or an account with write access to the repository).
- `DOCKERHUB_TOKEN`: a Docker Hub access token with write access to
  `kit101z/drone-runner-kube`.

Push this workflow to GitHub to enable automatic builds:

- Pull requests run tests and build both architectures without publishing.
- Pushes to `master` publish `kit101z/drone-runner-kube:latest`.
- Tags matching `v*` publish the exact Git tag, for example
  `kit101z/drone-runner-kube:v1.0.0`.
- **Actions > Docker image > Run workflow** builds and publishes the selected ref.
  Only the `master` branch updates `latest`.

Every published build also has a `sha-<full-commit-sha>` tag. Published tags point
to a multi-platform image containing both architectures. Version tags do not
overwrite `latest`.

```sh
docker pull kit101z/drone-runner-kube:latest
```

## Runner version

Use `--version` to inspect the binary without configuring or starting the runner:

```sh
drone-runner-kube --version
docker run --rm kit101z/drone-runner-kube:latest --version
kubectl -n <namespace> exec <runner-pod> -- /bin/drone-runner-kube --version
```

The output follows the Helm build-info format:

```text
version.BuildInfo{Version:"v1.0.0", GitCommit:"<full-commit-sha>", GitTreeState:"clean", GoVersion:"go1.16.15"}
```

GitHub Actions and Drone builds use `scripts/build.sh` to embed the source
metadata. `Version` comes from `git describe --tags --match 'v[0-9]*' --abbrev=7`:
an exact release tag is preserved, later commits include the distance and short
SHA (for example `v1.0.0-2-gabcdef0`), and a checkout without a reachable release
tag reports `devel`. `GitCommit` is the full HEAD SHA. `GitTreeState` is `dirty`
when tracked files differ from HEAD; otherwise it is `clean`. `GoVersion` is
the Go toolchain version compiled into the executable.

For local release builds, run `sh scripts/build.sh` (Linux amd64, arm64 and arm),
or select architectures with `sh scripts/build.sh amd64 arm64`. Fetch the full
history and release tags before building from a shallow clone. A plain
`go build` without metadata injection reports `Version:"devel"` and
`GitCommit:"unknown"`, `GitTreeState:"unknown"`.

## Release procedure

Run the changelog generator.

```BASH
docker run -it --rm -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator -u drone-runners -p drone-runner-kube -t <secret github token>
```

You can generate a token by logging into your GitHub account and going to Settings -> Personal access tokens.

Next we tag the PR's with the fixes or enhancements labels. If the PR does not fufil the requirements, do not add a label.

Run the changelog generator again with the future version according to semver.

```BASH
docker run -it --rm -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator -u drone-runners -p drone-runner-kube -t <secret token> --future-release v1.0.0
```

Create your pull request for the release. Get it merged then tag the release.
