package runners

import (
	"github.com/twitter/scoot/runner"
)

// Creates a new OutputCreator that will not save anything
func NewNullOutputCreator() *NullOutputCreator {
	return &NullOutputCreator{}
}

// NullOutputCreator pretends to create an Output but doesn't.
type NullOutputCreator struct{}

// Create creates a null output for id
func (c *NullOutputCreator) Create(id string) (runner.Output, error) {
	return &NullOutput{}, nil
}

type NullOutput struct{}

// URI returns file:///dev/null
func (o *NullOutput) URI() string {
	return "file:///dev/null"
}

// AsFile returns /dev/null
func (o *NullOutput) AsFile() string {
	return "/dev/null"
}

// Write implements io.Writer as a no-op
func (o *NullOutput) Write(p []byte) (int, error) {
	return len(p), nil
}

// Close implements io.Closer as a no-op
func (o *NullOutput) Close() error {
	return nil
}
