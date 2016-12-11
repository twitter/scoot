package local

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	osexecer "github.com/scootdev/scoot/runner/execer/os"
)

type localOutputCreator struct {
	tmp      *temp.TempDir
	hostname string
	handler  *endpoints.ResourceHandler
}

// Create a new OutputCreator
func NewOutputCreator(tmp *temp.TempDir, handler *endpoints.ResourceHandler) (runner.OutputCreator, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &localOutputCreator{tmp: tmp, hostname: hostname, handler: handler}, nil
}

func (s *localOutputCreator) Create(id string) (runner.Output, error) {
	f, err := s.tmp.TempFile(id)
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(f.Name())
	if err != nil {
		return nil, err
	}
	// We don't need a / between hostname and path because absolute paths start with /
	uri := fmt.Sprintf("file://%s%s", s.hostname, absPath)
	return &localOutput{f: f, hostname: s.hostname, absPath: absPath, uri: uri, handler: s.handler}, nil
}

type localOutput struct {
	f        *os.File
	hostname string
	absPath  string
	uri      string
	handler  *endpoints.ResourceHandler
}

func (o *localOutput) URI() string {
	return o.uri
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

func (o *localOutput) Register(namespace, name string) (uri string) {
	if o.handler != nil {
		o.uri = o.handler.AddResource(namespace, name, o.AsFile())
	}
	return o.uri
}

// Return an underlying Writer. Why? Because some methods type assert to
// a more specific type and are more clever (e.g., if it's an *os.File, hook it up
// directly to a new process's stdout/stderr.)
// We care about this cleverness, so Output both is-a and has-a Writer
func (o *localOutput) WriterDelegate() io.Writer {
	return o.f
}

var _ osexecer.WriterDelegater = (*localOutput)(nil)
