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

// Wraps another store and, when reading a bundle name that contains a forward slash, returns interpreted data:
//  see README.md for more detail.
// Caches bundles on disk instead of memory as they may be used as input to shelled git commands.
//
// TODO: git extraction logic should eventually move to gitdb as that's a more natural fit.
func MakeCachingBrowseStore(s Store, tmp *temp.TempDir) (Store, error) {
	if t, err := tmp.TempDir("browseStore"); err != nil {
		return nil, err
	} else {
		return &cachingBrowseStore{underlying: s, tmp: t}, nil
	}
}

type cachingBrowseStore struct {
	underlying Store
	tmp        *temp.TempDir
}

func (s *cachingBrowseStore) OpenForRead(name string) (io.ReadCloser, error) {
	paths := strings.SplitN(name, "/", 2)
	bundleName := paths[0]
	bundlePath := filepath.Join(s.tmp.Dir, bundleName)
	// Ensure that the bundle is available on disk.
	if _, err := os.Stat(bundlePath); err != nil {
		if reader, err := s.underlying.OpenForRead(bundleName); err != nil {
			return nil, err
		} else if writer, err := os.Create(bundlePath); err != nil {
			return nil, err
		} else if _, err := io.Copy(writer, reader); err != nil {
			return nil, err
		}
	}
	// There is no appended path, return the original bundle.
	if len(paths) == 1 {
		return os.Open(bundlePath)
	}
	// There is an appended path, try to extract it from the bundle.
	contentPath := paths[1]
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
	// Create an empty repo, unpack the bundle and remember the HEAD sha, then show the desired file@HEAD.
	str := "git init -q; SHA=$(git bundle unbundle %s | cut -d' ' -f1); git show $SHA:%s > content"
	cmd := exec.Command("bash", "-c", fmt.Sprintf(str, bundlePath, contentPath))
	cmd.Dir = tmp.Dir
	if err := cmd.Run(); err != nil {
		return nil, err
	} else {
		return os.Open(filepath.Join(tmp.Dir, "content"))
	}
}
