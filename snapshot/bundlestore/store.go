package bundlestore

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/scootdev/scoot/os/temp"
)

type StoreRead interface {
	Exists(name string) (bool, error)
	OpenForRead(name string) (io.ReadCloser, error)
}

type StoreWrite interface {
	Write(name string, data io.Reader) error
}

type Store interface {
	StoreRead
	StoreWrite
}

func MakeFileStore(dir *temp.TempDir) (*FileStore, error) {
	bundleDir, err := dir.FixedDir("bundles")
	if err != nil {
		return nil, err
	}

	return &FileStore{bundleDir.Dir}, nil
}

type FileStore struct {
	bundleDir string
}

func (s *FileStore) OpenForRead(name string) (io.ReadCloser, error) {
	bundlePath := filepath.Join(s.bundleDir, name)
	return os.Open(bundlePath)
}

func (s *FileStore) Exists(name string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.bundleDir, name))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *FileStore) Write(name string, data io.Reader) error {
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
