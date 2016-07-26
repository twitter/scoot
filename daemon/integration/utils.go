// Utilities for integration testing Scoot Daemon
package integration

import (
	"bytes"
	"github.com/scootdev/scoot/daemon/client/cli"
	"io"
	"os"
)

// Run cl with args, return its output
func Run(cl *cli.CliClient, args ...string) (string, string, error) {
	oldArgs := os.Args
	os.Args = append([]string{"scootcl"}, args...)
	defer func() {
		os.Args = oldArgs
	}()

	output := captureOutput()
	defer output.Reset()

	err := cl.Exec()
	stdout, stderr := output.WaitAndReset() // Reset early so we can use stdout/stderr and write to uncaptured stdout/stderr
	return stdout, stderr, err
}

// Below here is code to capture stdout and stderr
type output struct {
	stdout    *os.File
	stdoutCh  chan string
	stderr    *os.File
	stderrCh  chan string
	oldStdout *os.File
	oldStderr *os.File
}

// Capture stdour and stdout into a *output.
func captureOutput() *output {
	oldStdout, oldStderr := os.Stdout, os.Stderr
	stdout, stdoutCh := makeDiversion()
	stderr, stderrCh := makeDiversion()
	os.Stdout, os.Stderr = stdout, stderr
	return &output{stdout, stdoutCh, stderr, stderrCh, oldStdout, oldStderr}
}

// Creates a diversion: a file to write to, and a channel of string that will contain the contents written to it.
func makeDiversion() (*os.File, chan string) {
	reader, out, err := os.Pipe()
	if err != nil {
		panic("Cannot create pipe")
	}
	outCh := make(chan string)

	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, reader)
		outCh <- buf.String()
	}()

	return out, outCh
}

// Stop capturing output return the captured stdout and stderr
func (o *output) WaitAndReset() (string, string) {
	stdoutCh, stderrCh := o.stdoutCh, o.stderrCh
	o.Reset()
	return <-stdoutCh, <-stderrCh
}

// Stop capturing output.
func (o *output) Reset() {
	if o.stdout == nil {
		return
	}
	o.stdout.Close()
	o.stderr.Close()
	os.Stdout, os.Stderr = o.oldStdout, o.oldStderr
	// Zero out fields
	o.stdout, o.stderr, o.stdoutCh, o.stderrCh, o.oldStdout, o.oldStderr = nil, nil, nil, nil, nil, nil
}
