package cli_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/scootdev/scoot/daemon/client/cli"
	"github.com/scootdev/scoot/daemon/client/conn"
	"github.com/scootdev/scoot/daemon/integration"
	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

type errorDialer struct{}

func (d *errorDialer) Dial() (conn.Conn, error) {
	return nil, fmt.Errorf("errorDialer.Dial is error")
}

func (d *errorDialer) Close() error {
	return nil
}

func clientErrorDialer(t *testing.T) *cli.CliClient {
	cl, err := cli.NewCliClient(conn.NewCachingDialer(&errorDialer{}))
	if err != nil {
		t.Fatalf("Error constructing Cli Client: %v", err)
	}
	return cl
}

func clientForConn(t *testing.T, connection conn.Conn) *cli.CliClient {
	dialer := &connDialer{connection}
	cl, err := cli.NewCliClient(conn.NewCachingDialer(dialer))
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

func (d *connDialer) Close() error {
	return nil
}

func newFakeConn() conn.Conn {
	ex := execers.NewSimExecer()
	r := local.NewSimpleRunner(ex, snapshots.MakeInvalidFiler(), runners.NewNullOutputCreator())
	return &fakeConn{r, nil}
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

func (c *fakeConn) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	if c.err != nil {
		return runner.ProcessStatus{}, c.err
	}
	return c.runner.Run(cmd)
}

func (c *fakeConn) Status(run runner.RunId) (runner.ProcessStatus, error) {
	if c.err != nil {
		return runner.ProcessStatus{}, c.err
	}
	return c.runner.Status(run)
}

func (c *fakeConn) StatusAll() ([]runner.ProcessStatus, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.runner.StatusAll()
}

func (c *fakeConn) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	if c.err != nil {
		return runner.ProcessStatus{}, c.err
	}
	return c.runner.Abort(run)
}

func (c *fakeConn) Erase(run runner.RunId) error {
	return nil
}

func (c *fakeConn) Close() error {
	return nil
}

func (c *fakeConn) CreateSnapshot(path string) (snapshotId string, err error) {
	return "", nil
}

func (c *fakeConn) CheckoutSnapshot(checkoutId string, rootDir string) error {
	return nil
}

func (c *fakeConn) Poll(runIds []string, timeout int64, all bool) (statuses []protocol.RunStatus, err error) {
	return []protocol.RunStatus{}, nil
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
	stdout, stderr, err := integration.Run(cl, args...)
	if err != nil {
		t.Fatalf("Error running %v: %v", args, err)
	}
	return stdout, stderr
}

// Run cl with args and check it failed
func assertFailure(cl *cli.CliClient, t *testing.T, args ...string) (string, string, error) {
	stdout, stderr, err := integration.Run(cl, args...)
	if err == nil {
		t.Fatalf("No error running %v: %v", args, err)
	}
	return stdout, stderr, err
}
