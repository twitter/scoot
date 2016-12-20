package setup

import (
	"fmt"
	"os"
	"path"
	"strings"
)

// Builder is able to create Scoot binaries
type Builder interface {
	// Scheduler returns the path to the Scheduler binary (or an error if it can't be built)
	Scheduler() (string, error)
	// Worker returns the path to the Worker binary (or an error if it can't be built)
	Worker() (string, error)
}

const repoName = "github.com/scootdev/scoot"

// GoBuilder uses the go command-line tools to build Scoot
type GoBuilder struct {
	cmds *Cmds

	installed bool
	err       error
	schedBin  string
	workerBin string
}

// NewGoBuilder creates a GoBuilder
func NewGoBuilder(cmds *Cmds) *GoBuilder {
	return &GoBuilder{
		cmds: cmds,
	}
}

// install runs go install and caches the results
func (b *GoBuilder) install() {
	if b.installed {
		return
	}
	b.installed = true
	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		b.err = fmt.Errorf("GOPATH unset; cannot build")
		return
	}
	goPath = strings.Split(goPath, ":")[0]

	repoDir := path.Join(goPath, "src", repoName)

	cmd := b.cmds.Command("go", "install", "./binaries/...")
	cmd.Dir = repoDir
	if err := b.cmds.RunCmd(cmd); err != nil {
		b.err = err
		return
	}
	binDir := path.Join(goPath, "bin")
	b.schedBin = path.Join(binDir, "scheduler")
	b.workerBin = path.Join(binDir, "workerserver")
}

func (b *GoBuilder) Scheduler() (string, error) {
	b.install()
	return b.schedBin, b.err
}

func (b *GoBuilder) Worker() (string, error) {
	b.install()
	return b.workerBin, b.err
}
