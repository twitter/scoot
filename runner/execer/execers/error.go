package execers

import (
	"github.com/wisechengyi/scoot/runner/execer"
)

type ErrExecer struct {
	Err error
}

func (e *ErrExecer) Exec(command execer.Command) (execer.Process, error) {
	return nil, e.Err
}
