package version

import "runtime"

// These values are set by scripts/build.sh at link time.
var (
	version      = "devel"
	gitCommit    = "unknown"
	gitTreeState = "unknown"
)

// BuildInfo identifies the source and Go toolchain used to build the runner.
type BuildInfo struct {
	Version      string
	GitCommit    string
	GitTreeState string
	GoVersion    string
}

// Get returns the build metadata embedded in this executable.
func Get() BuildInfo {
	return BuildInfo{
		Version:      version,
		GitCommit:    gitCommit,
		GitTreeState: gitTreeState,
		GoVersion:    runtime.Version(),
	}
}
