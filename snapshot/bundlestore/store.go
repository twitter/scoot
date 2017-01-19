package bundlestore

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

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

func MakeFileStore(dir *temp.TempDir) (Store, error) {
	bundleDir, err := dir.FixedDir("bundles")
	if err != nil {
		return nil, err
	}

	return &fileStore{bundleDir.Dir}, nil
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

// Cache implementation
//TODO: implement and relocate.
type cache struct{}

func (c *cache) Read(name string) (bool, []byte) {
	return false, nil
}

func (c *cache) Write(name string, data []byte) {}

// Wraps another store and does simplistic caching of reads and exists calls.
func MakeCachingStore(s Store) Store {
	return &cachingStore{underlying: s}
}

type cachingStore struct {
	underlying Store
	cache      *cache
}

func (s *cachingStore) OpenForRead(name string) (io.ReadCloser, error) {
	if ok, data := s.cache.Read(name); ok {
		return ioutil.NopCloser(bytes.NewReader(data)), nil
	}
	return s.underlying.OpenForRead(name)
}

func (s *cachingStore) Exists(name string) (bool, error) {
	if ok, _ := s.cache.Read(name); ok {
		return true, nil
	}
	return s.underlying.Exists(name)
}

func (s *cachingStore) Write(name string, data io.Reader) error {
	return s.underlying.Write(name, data)
}
