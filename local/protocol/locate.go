package protocol

import (
	"fmt"
	"os"
	"path"
)

// Utilities to deal with Scoot instances.
// TODOs:
// read from $SCOOTDIR
// create scootdir if it doesn't exist (w/correct permissions)

// LocateScootDir locates a Scoot instance.
func LocateScootDir() (string, error) {
	homedir := os.Getenv("HOME")
	if homedir == "" {
		return "", fmt.Errorf("Cannot find home directory; $HOME unset")
	}
	return path.Join(homedir, ".scoot"), nil
}

// LocateSocket locates the path to the socket of a Scoot instance
func LocateSocket() (string, error) {
	scootdir, err := LocateScootDir()
	if err != nil {
		return "", err
	}
	return path.Join(scootdir, "socket"), nil
}

func SocketForDir(dir string) string {
	return path.Join(dir, "socket")
}
