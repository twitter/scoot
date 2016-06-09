package cli_test

import (
	"bytes"
	"fmt"
	"github.com/scootdev/scoot/local/client/cli"
	"github.com/scootdev/scoot/local/client/conn"
	"github.com/scootdev/scoot/runner"
	fakerunner "github.com/scootdev/scoot/runner/fake"
	"io"
	"os"
	"strings"
	"testing"
)

type errorDialer struct{}

func (d *errorDialer) Dial() (conn.Conn, error) {
	return nil, fmt.Errorf("errorDialer.Dial is error")
}

func clientErrorDialer(t *testing.T) *cli.CliClient {
	cl, err := cli.NewCliClient(&errorDialer{})
	if err != nil {
		t.Fatalf("Error constructing Cli Client: %v", err)
	}
	return cl
}

func clientForConn(t *testing.T, conn conn.Conn) *cli.CliClient {
	dialer := &connDialer{conn}
	cl, err := cli.NewCliClient(dialer)
	if err != nil {
		t.Fatalf("Error constructing Cli Client: %v", err)
	}
	return cl
}

type connDialer struct {
	conn conn.Conn
}

func (d *connDialer) Dial() (conn.Conn, error) {
	return d.conn, nil
}

func newFakeConn() conn.Conn {
	return &fakeConn{fakerunner.NewRunner(), nil}
}

type fakeConn struct {
	runner runner.Runner
	err    error
}

func (c *fakeConn) Echo(arg string) (string, error) {
	if c.err != nil {
		return "", c.err
	}
	return arg, nil
}

func (c *fakeConn) Run(cmd *runner.Command) (*runner.ProcessStatus, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.runner.Run(cmd)
}

func (c *fakeConn) Status(run runner.RunId) (*runner.ProcessStatus, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.runner.Status(run)
}

func (c *fakeConn) Close() error {
	return nil
}

func TestNoArgs(t *testing.T) {
	cl := clientErrorDialer(t)
	stdout, stderr := assertSuccess(cl, t, "")
	if stdout != "" || stderr != "" {
		t.Fatalf("blank should be empty: %v %v", stdout, stderr)
	}
}

func TestHelp(t *testing.T) {
	cl := clientErrorDialer(t)
	stdout, stderr := assertSuccess(cl, t, "help")
	if !strings.Contains(stdout, "Use \"scootcl [command] --help\" for more information about a command.") {
		t.Fatalf("Help stdout doesn't look right: %v", stdout)
	}
	if stderr != "" {
		t.Fatalf("Help stderr should be empty: %v", stderr)
	}
}

func TestEchoNoDialer(t *testing.T) {
	cl := clientErrorDialer(t)
	_, _, err := assertFailure(cl, t, "echo", "foo")
	if !strings.Contains(err.Error(), "errorDialer.Dial is error") {
		t.Fatalf("Echo error should mention errorDialer: %v", err)
	}
}

func TestEchoSuccess(t *testing.T) {
	conn := newFakeConn()
	cl := clientForConn(t, conn)
	stdout, _ := assertSuccess(cl, t, "echo", "foo")
	if stdout != "foo\n" {
		t.Fatalf("Echo didn't echo foo: %q", stdout)
	}
}

// Run cl with args and check it succeeded
func assertSuccess(cl *cli.CliClient, t *testing.T, args ...string) (string, string) {
	stdout, stderr, err := run(cl, args...)
	if err != nil {
		t.Fatalf("Error running %v: %v", args, err)
	}
	return stdout, stderr
}

// Run cl with args and check it failed
func assertFailure(cl *cli.CliClient, t *testing.T, args ...string) (string, string, error) {
	stdout, stderr, err := run(cl, args...)
	if err == nil {
		t.Fatalf("No error running %v: %v", args, err)
	}
	return stdout, stderr, err
}

// Run cl with args, return its output
func run(cl *cli.CliClient, args ...string) (string, string, error) {
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

func captureOutput() *output {
	oldStdout, oldStderr := os.Stdout, os.Stderr
	stdout, stdoutCh := makeDiversion()
	stderr, stderrCh := makeDiversion()
	os.Stdout, os.Stderr = stdout, stderr
	return &output{stdout, stdoutCh, stderr, stderrCh, oldStdout, oldStderr}
}

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

func (o *output) WaitAndReset() (string, string) {
	stdoutCh, stderrCh := o.stdoutCh, o.stderrCh
	o.Reset()
	return <-stdoutCh, <-stderrCh
}

func (o *output) Reset() {
	if o.stdout == nil {
		return
	}
	o.stdout.Close()
	o.stderr.Close()
	os.Stdout, os.Stderr = o.oldStdout, o.oldStderr
	// Zero
	o.stdout, o.stderr, o.stdoutCh, o.stderrCh, o.oldStdout, o.oldStderr = nil, nil, nil, nil, nil, nil
}
