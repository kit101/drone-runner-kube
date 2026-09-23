// Copyright 2019 Drone.IO Inc. All rights reserved.
// Use of this source code is governed by the Polyform License
// that can be found in the LICENSE file.

package command

import (
	"context"
	"fmt"
	"os"

	"github.com/drone-runners/drone-runner-kube/command/daemon"
	"github.com/drone-runners/drone-runner-kube/internal/version"

	"gopkg.in/alecthomas/kingpin.v2"
)

// empty context
var nocontext = context.Background()

// Command parses the command line arguments and then executes a
// subcommand program.
func Command() {
	app := kingpin.New("drone", "drone kubernetes runner")
	registerCompile(app)
	registerExec(app)
	daemon.Register(app)

	app.UsageWriter(os.Stdout)
	app.Version(fmt.Sprintf("%#v", version.Get()))
	kingpin.MustParse(app.Parse(os.Args[1:]))
}
