package runners

import (
	"github.com/wisechengyi/scoot/runner"
)

// Service makes a runner.Service from component parts.
type Service struct {
	runner.Controller
	runner.StatusReader
}
