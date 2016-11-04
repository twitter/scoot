package local

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	osexecer "github.com/scootdev/scoot/runner/execer/os"
)

type localOutputCreator struct {
	hostname string
}

// Create a new OutputCreator
func NewOutputCreator(tmp *temp.TempDir) (runner.OutputCreator, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &localOutputCreator{hostname: hostname}, nil
}

func (s *localOutputCreator) Create(id string) (runner.Output, error) {
	f, err := ioutil.TempFile("", id)
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(f.Name())
	if err != nil {
		return nil, err
	}
	// We don't need a / between hostname and path because absolute paths start with /
	return &localOutput{f: f, hostname: s.hostname, absPath: absPath}, nil
}

type localOutput struct {
	f        *os.File
	hostname string
	absPath  string
}

func (o *localOutput) URI() string {
	return fmt.Sprintf("file://%s%s", o.hostname, o.absPath)
}

func (o *localOutput) AsFile() string {
	return o.absPath
}

func (o *localOutput) Write(p []byte) (n int, err error) {
	return o.f.Write(p)
}

func (o *localOutput) Close() error {
	return o.f.Close()
}

// Return an underlying Writer. Why? Because some methods type assert to
// a more specific type and are more clever (e.g., if it's an *os.File, hook it up
// directly to a new process's stdout/stderr.)
// We care about this cleverness, so Output both is-a and has-a Writer
func (o *localOutput) WriterDelegate() io.Writer {
	return o.f
}

var _ osexecer.WriterDelegater = (*localOutput)(nil)
