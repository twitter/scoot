package integration_test

import (
	"fmt"
	"github.com/scootdev/scoot/daemon/client/cli"
	"github.com/scootdev/scoot/daemon/client/conn"
	"github.com/scootdev/scoot/daemon/integration"
	"github.com/scootdev/scoot/daemon/server"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestIntegration(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "scoot-listen-")
	if err != nil {
		t.Fatal("could not make temp directory", err)
	}
	defer os.RemoveAll(tempDir)

	scootDir := path.Join(tempDir, "scoot")
	err = os.Setenv("SCOOTDIR", scootDir)

	s, err := server.NewServer(nil)
	if err != nil {
		t.Fatal("could not make server", err)
	}

	l, err := server.Listen()

	go func() {
		s.Serve(l)
	}()

	if err != nil {
		t.Fatal("could not set env", err)
	}

	dialer, err := conn.UnixDialer()
	if err != nil {
		t.Fatal("could find Scoot Daemon", err)
	}

	err = testEcho(dialer)
}

func testEcho(dialer conn.Dialer) error {
	stdout, _, err := run(dialer, "echo", "foo")
	if err != nil {
		return fmt.Errorf("error echo'ing: %v", err)
	}
	if stdout != "foo\n" {
		return fmt.Errorf("Echo didn't echo foo: %q", stdout)
	}
	return nil
}

func run(dialer conn.Dialer, args ...string) (string, string, error) {
	cl, err := cli.NewCliClient(conn.NewCachingDialer(dialer))
	if err != nil {
		return "", "", err
	}
	defer cl.Close()
	return integration.Run(cl, args...)
}
