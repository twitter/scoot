package bundlestore

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/scootdev/scoot/os/temp"
)

// Create a fixed dir in tmp.
func MakeFileStoreInTemp(tmp *temp.TempDir) (Store, error) {
	bundleDir, err := tmp.FixedDir("bundles")
	if err != nil {
		return nil, err
	}
	log.Println("!!!!!!!", bundleDir.Dir)
	return MakeFileStore(bundleDir.Dir)

}

func MakeFileStore(dir string) (Store, error) {
	return &fileStore{dir}, nil
}

type fileStore struct {
	bundleDir string
}

func (s *fileStore) OpenForRead(name string) (io.ReadCloser, error) {
	bundlePath := filepath.Join(s.bundleDir, name)
	return os.Open(bundlePath)
}

func (s *fileStore) Exists(name string) (bool, error) {
	if strings.Contains(name, "/") {
		return false, errors.New("'/' not allowed in name unless reading bundle contents.")
	}
	_, err := os.Stat(filepath.Join(s.bundleDir, name))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *fileStore) Write(name string, data io.Reader) error {
	if strings.Contains(name, "/") {
		return errors.New("'/' not allowed in name unless reading bundle contents.")
	}
	bundlePath := filepath.Join(s.bundleDir, name)
	log.Printf("Writing %s to %s", name, bundlePath)
	f, err := os.Create(bundlePath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, data); err != nil {
		return err
	}
	return nil
}
