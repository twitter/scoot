package bundlestore

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/os/temp"
)

// Create a fixed dir in tmp.
// Note: this implementation does not currently support TTL.
func MakeFileStoreInTemp(tmp *temp.TempDir) (*FileStore, error) {
	bundleDir, err := tmp.FixedDir("bundles")
	if err != nil {
		return nil, err
	}
	return MakeFileStore(bundleDir.Dir)
}

func MakeFileStore(dir string) (*FileStore, error) {
	return &FileStore{dir}, nil
}

type FileStore struct {
	bundleDir string
}

func (s *FileStore) OpenForRead(name string) (io.ReadCloser, error) {
	bundlePath := filepath.Join(s.bundleDir, name)
	return os.Open(bundlePath)
}

func (s *FileStore) Exists(name string) (bool, error) {
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

func (s *FileStore) Write(name string, data io.Reader, ttl *TTLValue) error {
	if strings.Contains(name, "/") {
		return errors.New("'/' not allowed in name unless reading bundle contents.")
	}
	bundlePath := filepath.Join(s.bundleDir, name)
	log.Infof("Writing %s to %s", name, bundlePath)
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

func (s *FileStore) Root() string {
	return s.bundleDir
}
