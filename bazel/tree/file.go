// download and read in, write out and upload.
// transparent local cache.
// interop with CAS server.
// CLI (via bzutil)
package tree

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	scootproto "github.com/twitter/scoot/common/proto"
)

// TODO
//	* symlinks:
//		in ReadDir - should be straightforward - just set up data structure & hash based on target, etc
//		slurping and materializing - have to place wherever the target said to
//		absolute vs relative - we will probably support relative only
//		there will be cascading changes to invoker, etc to support symlinks and integrate this change.
//	* read-in uploading
//		all intermittent resources get ingested
//	* materializing (fetching + downloading)
//		starting with a dir root, fetch that, then recursively unravel
//		set up each file/dir/symlink by writing out binary data to that relative file
//		this is also GetTree, sortof (but doesn't create file state locally)
//	* local caching
//		anytime would fetch from a server, check cache first (also need to populate)
//		need some settings around local dir (size, cleaning, etc)
//		needs get/put/clean interfaces
//	* organize and doc and comment

func ReadFile(path string) (*remoteexecution.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	l, err := io.Copy(h, f)
	if err != nil {
		return nil, err
	}

	return &remoteexecution.Digest{Hash: fmt.Sprintf("%x", h.Sum(nil)), SizeBytes: l}, nil
}

func ReadDirectory(path string) (*remoteexecution.Digest, error) {
	d, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	files, err := d.Readdir(0)
	if err != nil {
		return nil, err
	}

	directory := &remoteexecution.Directory{}

	for _, f := range files {
		if f.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("symlink file %s unsupported", f.Name())
		} else if f.IsDir() {
			digest, err := ReadDirectory(filepath.Join(path, f.Name()))
			if err != nil {
				return nil, err
			}
			dnode := &remoteexecution.DirectoryNode{
				Name:   f.Name(),
				Digest: digest,
			}
			directory.Directories = append(directory.Directories, dnode)
		} else {
			digest, err := ReadFile(filepath.Join(path, f.Name()))
			if err != nil {
				return nil, err
			}
			fnode := &remoteexecution.FileNode{
				Name:         f.Name(),
				Digest:       digest,
				IsExecutable: (f.Mode()&1 != 0),
			}
			directory.Files = append(directory.Files, fnode)
		}
	}

	sort.Slice(directory.Files, func(i, j int) bool { return directory.Files[i].Name < directory.Files[j].Name })
	sort.Slice(directory.Directories, func(i, j int) bool { return directory.Directories[i].Name < directory.Directories[j].Name })
	sort.Slice(directory.Symlinks, func(i, j int) bool { return directory.Symlinks[i].Name < directory.Symlinks[j].Name })

	hash, size, err := scootproto.GetSha256(directory)
	if err != nil {
		return nil, err
	}
	return &remoteexecution.Digest{Hash: hash, SizeBytes: size}, nil
}
