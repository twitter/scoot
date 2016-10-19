package server

// interface defining the Scoot API

import (
	"github.com/scootdev/scoot/runner"
)

type LocalPath string
type RunCommand []string

type OutputStrategy string // TODO implement

type Scoot interface {
	Run(id string, cmd RunCommand, outputStrategy OutputStrategy) (runner.RunId, error)
}
