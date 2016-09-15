package runners

import (
	"github.com/scootdev/scoot/runner"
)

type nullOutputCreator struct{}

func NewNullOutputCreator() runner.OutputCreator {
	return &nullOutputCreator{}
}

func (c *nullOutputCreator) Create(id string) (runner.Output, error) {
	return &nullOutput{}, nil
}

type nullOutput struct{}

func (o *nullOutput) URI() string {
	return "file:///dev/null"
}

func (o *nullOutput) Write(p []byte) (int, error) {
	return len(p), nil
}

func (o *nullOutput) Close() error {
	return nil
}
