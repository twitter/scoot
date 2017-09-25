package execers

import (
	"github.com/twitter/scoot/runner/execer"
)

// InterceptExecer is a Composite Execer. For each command c, if
// Condition(c), InterceptExecer delegates to Interceptor, otherwise Default
type InterceptExecer struct {
	Condition   func(execer.Command) bool
	Interceptor execer.Execer
	Default     execer.Execer
}

func (e *InterceptExecer) Exec(command execer.Command) (execer.Process, error) {
	if e.Condition(command) {
		return e.Interceptor.Exec(command)
	} else {
		return e.Default.Exec(command)
	}
}

// Returns whether cmd's first arg starts with UseSimExecerArg
func StartsWithSimExecer(cmd execer.Command) bool {
	return len(cmd.Argv) > 0 && cmd.Argv[0] == UseSimExecerArg
}

// A placeholder string that indicates a command should be run on SimExecer
const UseSimExecerArg = "#! sim execer"

// Create an InterceptExecer that will send cmd's to simExecer or delegate
func MakeSimExecerInterceptor(simExecer, delegate execer.Execer) execer.Execer {
	return &InterceptExecer{
		Condition:   StartsWithSimExecer,
		Interceptor: simExecer,
		Default:     delegate,
	}
}
