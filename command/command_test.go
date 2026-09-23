package command

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func TestCommandVersionAndHelp(t *testing.T) {
	for _, test := range []struct {
		name string
		arg  string
		want []string
	}{
		{
			name: "version",
			arg:  "--version",
			want: []string{
				`Version:"devel"`,
				`GitCommit:"unknown"`,
				`GitTreeState:"unknown"`,
				`GoVersion:"` + runtime.Version() + `"`,
			},
		},
		{
			name: "help",
			arg:  "--help",
			want: []string{"--version", "Show application version."},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestCommandHelperProcess")
			cmd.Env = append(os.Environ(), "GO_WANT_COMMAND_HELPER=1", "COMMAND_ARG="+test.arg)
			output, err := cmd.Output()
			if err != nil {
				t.Fatalf("Command(%q) failed: %v\n%s", test.arg, err, output)
			}
			for _, want := range test.want {
				if !strings.Contains(string(output), want) {
					t.Errorf("Command(%q) output %q does not contain %q", test.arg, output, want)
				}
			}
		})
	}
}

func TestBuildScriptMetadata(t *testing.T) {
	cmd := exec.Command("sh", "../scripts/build_test.sh")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build metadata test failed: %v\n%s", err, output)
	}
}

func TestCommandHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_COMMAND_HELPER") != "1" {
		return
	}
	os.Args = []string{"drone", os.Getenv("COMMAND_ARG")}
	Command()
}
