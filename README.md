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
