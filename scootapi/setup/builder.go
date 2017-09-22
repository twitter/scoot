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
	// ApiServer returns the path to the Worker binary (or an error if it can't be built)
	ApiServer() (string, error)
}

const repoName = "github.com/twitter/scoot"

// GoBuilder uses the go command-line tools to build Scoot
type GoBuilder struct {
	cmds *Cmds

	installed    bool
	err          error
	schedBin     string
	workerBin    string
	apiserverBin string
}

// NewGoBuilder creates a GoBuilder
func NewGoBuilder(cmds *Cmds) *GoBuilder {
	return &GoBuilder{
		cmds: cmds,
	}
}

func GoPath() (string, error) {
	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		return "", fmt.Errorf("GOPATH unset; cannot build")
	}
	return strings.Split(goPath, ":")[0], nil
}

func RepoRoot(goPath string) string {
	return path.Join(goPath, "src", repoName)
}

// install runs go install and caches the results
func (b *GoBuilder) install() {
	if b.installed {
		return
	}
	b.installed = true
	goPath, err := GoPath()
	if err != nil {
		return
	}
	repoDir := RepoRoot(goPath)

	cmd := b.cmds.Command("go", "install", "./binaries/...")
	cmd.Dir = repoDir
	if err := b.cmds.RunCmd(cmd); err != nil {
		b.err = err
		return
	}
	binDir := path.Join(goPath, "bin")
	b.schedBin = path.Join(binDir, "scheduler")
	b.workerBin = path.Join(binDir, "workerserver")
	b.apiserverBin = path.Join(binDir, "apiserver")
}

func (b *GoBuilder) Scheduler() (string, error) {
	b.install()
	return b.schedBin, b.err
}

func (b *GoBuilder) Worker() (string, error) {
	b.install()
	return b.workerBin, b.err
}

func (b *GoBuilder) ApiServer() (string, error) {
	b.install()
	return b.apiserverBin, b.err
}
