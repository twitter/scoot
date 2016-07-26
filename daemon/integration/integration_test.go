package integration_test

import (
	"flag"
	"github.com/scootdev/scoot/daemon/client/cli"
	"github.com/scootdev/scoot/daemon/client/conn"
	"github.com/scootdev/scoot/daemon/integration"
	"github.com/scootdev/scoot/daemon/server"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
)

var s *server.Server

func TestEcho(t *testing.T) {
	stdout, _, err := run("echo", "foo")
	if err != nil {
		t.Fatalf("error echo'ing: %v", err)
	}
	if stdout != "foo\n" {
		t.Fatalf("Echo didn't echo foo: %q", stdout)
	}
}

func run(args ...string) (string, string, error) {
	dialer, err := conn.UnixDialer()
	if err != nil {
		return "", "", err
	}

	cl, err := cli.NewCliClient(conn.NewCachingDialer(dialer))
	if err != nil {
		return "", "", err
	}
	defer cl.Close()
	return integration.Run(cl, args...)
}

func TestMain(m *testing.M) {
	flag.Parse()
	tempDir, err := ioutil.TempDir("", "scoot-listen-")
	if err != nil {
		log.Fatal("could not make temp directory", err)
	}
	defer os.RemoveAll(tempDir)

	scootDir := path.Join(tempDir, "scoot")
	err = os.Setenv("SCOOTDIR", scootDir)

	s, err = server.NewServer(nil)
	if err != nil {
		log.Fatal("could not make server", err)
	}

	l, err := server.Listen()
	go func() {
		s.Serve(l)
	}()

	defer s.Stop()

	os.Exit(m.Run())
}
