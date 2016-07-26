package server

import (
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

	scootDir := path.Join(tempDir, "scoot")

	socketPath := path.Join(scootDir, "socket")

	l, err := listen(socketPath)
	if err != nil {
		t.Fatalf("could not listen: %v", err)
	}

	_, err = listen(socketPath)

	if err == nil {
		t.Fatalf("should not be able to listen again")
	}

	l.Close()

	l, err = listen(socketPath)

	if err != nil {
		t.Fatalf("could not replace dead server")
	}
}
