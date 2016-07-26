package server_test

import (
	"github.com/scootdev/scoot/daemon/server"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestListen(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "scoot-listen-")

	if err != nil {
		t.Fatalf("could not make temp directory: %v", err)
	}

	defer os.RemoveAll(tempDir)

	os.Setenv("SCOOTDIR", path.Join(tempDir, "scoot"))

	l, err := server.Listen()
	if err != nil {
		t.Fatalf("could not listen: %v", err)
	}

	_, err = server.Listen()

	if err == nil {
		t.Fatalf("should not be able to listen again")
	}

	l.Close()

	l, err = server.Listen()

	if err != nil {
		t.Fatalf("could not replace dead server")
	}
}
