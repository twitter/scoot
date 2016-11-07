package runners

import (
	"github.com/scootdev/scoot/runner"
)

// Creates a new OutputCreator that will not save anything
func NewNullOutputCreator() runner.OutputCreator {
	return &nullOutputCreator{}
}

type nullOutputCreator struct{}

// Create creates a null output for id
func (c *nullOutputCreator) Create(id string) (runner.Output, error) {
	return &nullOutput{}, nil
}

type nullOutput struct{}

// URI returns file:///dev/null
func (o *nullOutput) URI() string {
	return "file:///dev/null"
}

// AsFile returns /dev/null
func (o *nullOutput) AsFile() string {
	return "/dev/null"
}

// Write implements io.Writer as a no-op
func (o *nullOutput) Write(p []byte) (int, error) {
	return len(p), nil
}

// Close implements io.Closer as a no-op
func (o *nullOutput) Close() error {
	return nil
}
