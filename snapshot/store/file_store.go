package store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	"io/ioutil"
)

// Create a fixed dir in tmp.
// Note: this implementation does not currently support TTL.
func MakeFileStoreInTemp() (*FileStore, error) {
	bundleDir, err := ioutil.TempDir("", "bundles")
	if err != nil {
		return nil, err
	}
	return MakeFileStore(bundleDir)
}

func MakeFileStore(dir string) (*FileStore, error) {
	log.Infof("Making new FileStore at dir: %s", dir)
	return &FileStore{dir}, nil
}

type FileStore struct {
	bundleDir string
}

func (s *FileStore) OpenForRead(name string) (*Resource, error) {
	bundlePath := filepath.Join(s.bundleDir, name)
	r, err := os.Open(bundlePath)
	if err != nil {
		return nil, err
	}
	fi, err := r.Stat()
	if err != nil {
		return nil, err
	}
	return NewResource(r, fi.Size(), nil), err
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

func (s *FileStore) Write(name string, resource *Resource) error {
	if resource == nil {
		log.Info("Writing nil resource is a no op.")
		return nil
	}
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

	if _, err := io.Copy(f, resource); err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	resource.Length = fi.Size()
	return nil
}

func (s *FileStore) Root() string {
	return s.bundleDir
}
