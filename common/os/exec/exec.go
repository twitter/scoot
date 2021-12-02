// Package exec provides extended functionality interfaces to os/exec as well as exec utilities
package exec

import (
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"strings"
	"syscall"
)

type (
	// OsExec provides an interface around os/exec.Command to support injecting fake
	// exec functionality
	OsExec interface {
		// Command creates a Cmd interface with the path to the command to run
		// 'cmd' and the command arguments set.
		//
		// If name contains no path separators, Command uses os/exec.LookPath() to resolve
		// the path to a complete name if possible. Otherwise it uses name
		// directly.
		//
		// The returned Cmd's Args field is constructed from the command name
		// followed by the elements of arg, so arg should not include the command
		// name itself. For example, Command("echo", "hello")
		Command(cmd string, args ...string) Cmd
	}

	// we provide an adaptor for Cmd on this struct
	defaultOsExec struct{}

	/* (jsimms) this should perhaps return a different interface after Run() has been
	 * called. That way we can avoid having a stateful Cmd? */

	/* TODO(jsimms): implement Wait() et. al. */

	// Cmd wraps the os/exec.Cmd struct with our own interface
	Cmd interface {
		// Path returns the path to the executable to run
		Path() string

		// Args returns the arguments to give the executable.
		// A copy of the args are returned to the caller, so this should not be
		// used to modify the arguments.
		Args() []string

		// Output runs the command and returns its standard output.
		Output() ([]byte, error)

		// Run starts the specified command and waits for it to complete.
		//
		// The returned error is nil if the command runs, has no problems
		// copying stdin, stdout, and stderr, and exits with a zero exit
		// status.
		//
		// If the command fails to run or doesn't complete successfully, the
		// error is of type ExitError. Other error types may be
		// returned for I/O problems.
		Run() error

		// Start starts the specified command but does not wait for it to complete.
		//
		// The Wait method will return the exit code and release associated resources
		// once the command exits.
		Start() error

		// Wait waits for the command to exit. It must have been started by Start.
		//
		// The returned error is nil if the command runs, has no problems copying stdin,
		// stdout, and stderr, and exits with a zero exit status.
		//
		// If the command fails to run or doesn't complete successfully, the error is of
		// type *ExitError. Other error types may be returned for I/O problems.
		//
		// If c.Stdin is not an *os.File, Wait also waits for the I/O loop copying from
		// c.Stdin into the process's standard input to complete.
		//
		// Wait releases any resources associated with the Cmd.
		Wait() error

		// Enables/disables the setsid flag for the child process, disabled by default.
		// Setsid forces the child process into its own session, making it easier and more
		// reliable to manage signals for to terminate the process and all of its children.
		//
		// Only available for Linux, all other platforms will just no-op.
		SetSession(enable bool)

		// SetStdin sets the standard input of the subproces to read from the given
		// io.Reader
		SetStdin(io.Reader)

		// SetStdout sets the stdout of the process to write to the given io.Writer
		SetStdout(io.Writer)

		// SetStderr sets the stderr of the process to write to the given io.Writer
		SetStderr(io.Writer)

		// String returns a human-readable description of c. It is intended only for debugging.
		String() string

		// Process returns the underlying os.Process object once the command has
		// been started, and nil if it has not been started
		Process() *os.Process

		// ProcessState returns the underlying ProcessState once the process has
		// exited and nil if it has not
		ProcessState() *os.ProcessState

		// GetEnv returns an Env of the current execution environment for the process.
		// If the underlying Cmd's env is nil, we return the current processes' execution
		// environment as an Env
		//
		// changes to the values in Env will not affect the map set for the Cmd or
		// for the current process. You must modify Env and then call SetEnv with
		// the modified Env instance
		//
		// If there is an error parsing the current environment it will be returned
		// in error.
		GetEnv() (Env, error)

		// SetEnv allows the user to set the execution environment for the process.
		SetEnv(m Env)

		// GetDir returns the underlying working directory of the command
		GetDir() string

		// SetDir sets the underlying working directory of the command
		SetDir(string)
	}

	// Env provides an interface for manipulating the environment the subprocess
	// will execute in. It's provides a similar API to native maps.
	Env interface {
		// Set an environment variable 'k' to the value 'v'
		Set(k string, v string)

		// Get an environment variable 'k' as a string. If unset 'ok' will be false
		Get(k string) (v string, ok bool)

		// Unset removes the key from the environment
		Unset(k string)

		// ToSlice returns the Env as a slice of strings in the same format as os.Environ
		ToSlice() []string
	}

	processEnv struct {
		envMap map[string]string
	}

	// ExitError provides our own interface around process termination to allow for
	// mocking in tests.
	//
	// Since this is an interface, and not a concrete implementation (i.e. a struct),
	// you need to check a cast of the error you receive:
	//
	//   err := NewOsExec().Command("false").Run()
	//   if exitErrror, ok := err.(ExitError); ok {
	//     /* we have an exit error here, so we can call methods on it */
	//   }
	ExitError interface {
		// Exited reports if the process has exited by calling the libc exit() function. If a process
		// is terminated due to an uncaught signal, Exited will return false.
		Exited() bool

		// Pid returns the process id of the process which this information is relevant to
		Pid() int

		// ExitStatus returns the numerical exit status code from the process if Exited() is true.
		// If Exited returns false, this function will return -1
		ExitStatus() int

		// Signaled returns true if the process died because of an untrapped signal
		Signaled() bool

		// Signal returns the signal number that killed the process if Signaled() is true.
		// This function returns syscall.Signal(-1) if Signaled() is false.
		Signal() syscall.Signal

		// Error satisfies the error interface
		Error() string

		// Path contains the path from the Cmd that returned this error
		Path() string

		// Args contains the args from the Cmd that returned this error
		Args() []string
	}

	// adapter to the Cmd interface for the exec.Cmd struct
	cmdAdapter struct {
		cmd *osexec.Cmd
	}

	// ExitErrorAdapter wraps an os/exec.ExitError and provides access to the
	// exit status.
	exitErrorAdapter struct {
		err  *osexec.ExitError
		ws   syscall.WaitStatus
		path string
		args []string
	}
)

// implements assertions
var (
	_ ExitError = &exitErrorAdapter{}
	_ Cmd       = &cmdAdapter{}
	_ Env       = &processEnv{}
)

// NewOsExec creates a default OsExec instance
func NewOsExec() OsExec {
	return &defaultOsExec{}
}

/* Default OsExec impl */

func (d *defaultOsExec) Command(cmd string, args ...string) Cmd {
	return &cmdAdapter{cmd: osexec.Command(cmd, args...)}
}

/* adapter for exec.ExitError to our version */

func wrapExitError(cmd Cmd, err error) error {
	if err == nil {
		return nil
	}

	if ex, ok := err.(*osexec.ExitError); ok {
		if ws, ok := ex.Sys().(syscall.WaitStatus); ok {
			return &exitErrorAdapter{
				err:  ex,
				ws:   ws,
				path: cmd.Path(),
				args: cmd.Args(),
			}
		}
	}
	return err
}

func (e *exitErrorAdapter) Exited() bool           { return e.ws.Exited() }
func (e *exitErrorAdapter) Pid() int               { return e.err.Pid() }
func (e *exitErrorAdapter) ExitStatus() int        { return e.ws.ExitStatus() }
func (e *exitErrorAdapter) Signaled() bool         { return e.ws.Signaled() }
func (e *exitErrorAdapter) Signal() syscall.Signal { return e.ws.Signal() }
func (e *exitErrorAdapter) Error() string          { return e.err.Error() }
func (e *exitErrorAdapter) Path() string           { return e.path }
func (e *exitErrorAdapter) Args() []string         { return e.args }

/* Cmd adapter for exec.Cmd */

func (c *cmdAdapter) Output() ([]byte, error) {
	bytes, err := c.cmd.Output()
	if err != nil {
		return bytes, wrapExitError(c, err)
	}
	return bytes, nil
}

// SetSession enables setsid to be called after the child process
// is forked. This makes that child process the leader in its own
// process group, which makes killing it and all its subprocs
// more reliable.
//
// This functionality may not be available on all platforms, so
// make sure SysProcAttr actually exists first.
func (c *cmdAdapter) SetSession(enable bool) {
	// TODO(jsimms): change this so that cmdAdapter has a SysProcAttr() method that returns
	// a pointer to this struct, that way we don't have to wrap each bit of the struct in
	// getters and setters.
	if c.cmd.SysProcAttr != nil {
		c.cmd.SysProcAttr.Setsid = enable
	}
}

func (c *cmdAdapter) Run() error   { return wrapExitError(c, c.cmd.Run()) }
func (c *cmdAdapter) Start() error { return c.cmd.Start() }
func (c *cmdAdapter) Wait() error  { return wrapExitError(c, c.cmd.Wait()) }

func (c *cmdAdapter) Path() string                   { return c.cmd.Path }
func (c *cmdAdapter) SetStdin(r io.Reader)           { c.cmd.Stdin = r }
func (c *cmdAdapter) SetStdout(w io.Writer)          { c.cmd.Stdout = w }
func (c *cmdAdapter) SetStderr(w io.Writer)          { c.cmd.Stderr = w }
func (c *cmdAdapter) String() string                 { return c.cmd.String() }
func (c *cmdAdapter) Process() *os.Process           { return c.cmd.Process }
func (c *cmdAdapter) ProcessState() *os.ProcessState { return c.cmd.ProcessState }

func (c *cmdAdapter) Args() []string {
	// return a copy of the Args slice to prevent direct modification by the user
	return append([]string(nil), c.cmd.Args...)
}

func (c *cmdAdapter) GetEnv() (Env, error) { return NewEnv(c.cmd.Env) }
func (c *cmdAdapter) SetEnv(env Env)       { c.cmd.Env = env.ToSlice() }
func (c *cmdAdapter) GetDir() string       { return c.cmd.Dir }
func (c *cmdAdapter) SetDir(dir string)    { c.cmd.Dir = dir }

/* Env implementation */

func splitEnvStrings(env []string) (map[string]string, error) {
	m := make(map[string]string)
	for _, s := range env {
		xs := strings.SplitN(s, "=", 2)
		if len(xs) != 2 {
			return nil, fmt.Errorf("splitEnvStrings: invalid environment string: %q", s)
		}
		m[xs[0]] = xs[1]
	}
	return m, nil
}

// NewEnv takes a []string as from os.Environ and returns a map
// if env is Nil, the current process environment will be used
func NewEnv(env []string) (Env, error) {
	var err error
	var m map[string]string

	if env == nil {
		env = os.Environ()
	}

	m, err = splitEnvStrings(env)
	if err != nil {
		return nil, err
	}

	return &processEnv{envMap: m}, nil
}

func (p *processEnv) Get(k string) (string, bool) {
	v, ok := p.envMap[k]
	return v, ok
}

func (p *processEnv) Unset(k string) { delete(p.envMap, k) }

func (p *processEnv) Set(k string, v string) { p.envMap[k] = v }

func (p *processEnv) ToSlice() []string {
	var s []string
	for k, v := range p.envMap {
		s = append(s, strings.Join([]string{k, v}, "="))
	}
	return s
}
