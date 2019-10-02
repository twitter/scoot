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
//		slurping and materializing - have to place wherever the target said to
//		absolute vs relative - we will probably support relative only
//			*** relative to the directory that the link is in *** (symlink's parent directory)
//			must verify that the dest is within the topdir we are operating in, or else this doesn't make sense?
//			or do we have to verify anything? just do it, if the path points to the wrong place, the system should
//			prevent this in a better way, e.g. the chroot or container or whatever prevents mucking around
//		there will be cascading changes to invoker, etc to support symlinks and integrate this change.
//	* read-in uploading
//		all intermittent resources get ingested
//		queue of ingests/uploads as data is read in
//		queue of ops drained (w/ settings on i.e. parallelism), reported back
//		values also saved in local cache (transparent LRU API)
//	* materializing (fetching + downloading)
//		starting with a dir root, fetch that, then recursively unravel
//		set up each file/dir/symlink by writing out binary data to that relative file
//			note that symlinks don't matter, just write out the actual file (that means saving & materializing isn't stable)
//		this is also GetTree, sortof (but doesn't create file state locally)
//	* local caching
//		anytime would fetch from a server, check cache first (also need to populate)
//		need some settings around local dir (size, cleaning, etc)
//		needs get/put/clean interfaces
//		nothing great on gh. just store stuff in file buckets. clean based on access time (via cleaner)
//		memory-mapped DB? pants uses LMDB. would prefer cleanability (lmdb isn't, really, until you have to purge everything)
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
			target, err := os.Readlink(filepath.Join(path, f.Name()))
			if err != nil {
				return nil, err
			}
			target = filepath.ToSlash(filepath.Clean(target))
			snode := &remoteexecution.SymlinkNode{
				Name:   f.Name(),
				Target: target,
			}
			directory.Symlinks = append(directory.Symlinks, snode)
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
