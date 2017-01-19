package bundlestore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/scootdev/scoot/os/temp"
)

// Wraps another store and, when reading and when bundle name contains a slash, returns interpreted data.
// The slash is considered the start of a root path to extract from the original bundle data.
// Bundle must have nil basis for extraction to work.
// Caches bundles on disk to use as input to git commands.
func MakeCachingBrowseStore(s Store, tmp *temp.TempDir) Store {
	return &cachingBrowseStore{underlying: s, tmp: tmp}
}

type cachingBrowseStore struct {
	underlying Store
	tmp        *temp.TempDir
}

func (s *cachingBrowseStore) OpenForRead(name string) (io.ReadCloser, error) {
	paths := strings.SplitN(name, "/", 2)
	bundleName := paths[0]
	contentPath := paths[1]
	bundlePath := filepath.Join(s.tmp.Dir, bundleName)
	if _, err := os.Stat(bundlePath); err != nil {
		if reader, err := s.underlying.OpenForRead(bundleName); err != nil {
			return nil, err
		} else if file, err := os.Create(bundlePath); err != nil {
			return nil, err
		} else if _, err := io.Copy(file, reader); err != nil {
			return nil, err
		}
	}
	if len(paths) == 1 {
		return os.Open(bundlePath)
	}
	return s.extract(bundlePath, contentPath)
}

func (s *cachingBrowseStore) Exists(name string) (bool, error) {
	if strings.Contains(name, "/") {
		return false, errors.New("'/' not allowed in name unless reading bundle contents.")
	}
	return s.underlying.Exists(name)
}

func (s *cachingBrowseStore) Write(name string, data io.Reader) error {
	if strings.Contains(name, "/") {
		return errors.New("'/' not allowed in name unless reading bundle contents.")
	}
	return s.underlying.Write(name, data)
}

func (s *cachingBrowseStore) extract(bundlePath, contentPath string) (io.ReadCloser, error) {
	tmp, err := s.tmp.TempDir("gitScratch")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp.Dir)
	str := "git init -q; SHA=$(git bundle unbundle %s | cut -d' ' -f1); git show $SHA:%s > content"
	cmd := exec.Command("bash", "-c", fmt.Sprintf(str, bundlePath, contentPath))
	cmd.Dir = tmp.Dir
	if err := cmd.Run(); err != nil {
		return nil, err
	} else {
		return os.Open(filepath.Join(tmp.Dir, "content"))
	}
}
