package protocol

import (
	"fmt"
	"os"
	"path"
)

// Utilities to deal with Scoot instances.

// LocateScootDir locates a Scoot instance.
func LocateScootDir() (string, error) {
	scootdir := os.Getenv("SCOOTDIR")
	if scootdir != "" {
		return scootdir, nil
	}
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
