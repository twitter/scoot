package minfuse

import (
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"syscall"
	"time"

	fs "github.com/twitter/scoot/fs/min/interface"
	"github.com/twitter/scoot/fs/perf"
	"github.com/twitter/scoot/fuse"
	"github.com/twitter/scoot/snapshot"
)

const blockSize uint32 = 4096

var Trace bool = false

func NewSlimMinFs(snap snapshot.Snapshot) fs.FS {
	return &slimMinFS{snap, os.Getuid(), os.Getgid(), blockSize}
}

type slimMinFS struct {
	snap      snapshot.Snapshot
	ownerUID  int
	ownerGID  int
	blockSize uint32
}

func (fs *slimMinFS) Root() (fs.Node, error) {
	fi, err := fs.snap.Lstat("")
	if err != nil {
		return nil, err
	}

	attr := makeAttr(fi, fs.ownerUID, fs.ownerGID, fs.blockSize)

	return &minNode{fs, "", attr}, nil
}

type minNode struct {
	fs   *slimMinFS
	p    string
	attr fuse.Attr
}

func (n *minNode) Attr() (fuse.Attr, error) {
	return n.attr, nil
}

func (n *minNode) Readlink() (string, error) {
	return n.fs.snap.Readlink(n.p)
}

func (n *minNode) Lookup(name string) (fs.Node, error) {
	if Trace {
		log.Info("Min: Lookup entry ", n.p, name)
		defer log.Info("Min: Lookup exit ", n.p, name)
	}

	// minNode stores the joined path so pass discards=false.
	p := perf.UnsafePathJoin(false, n.p, name)
	fi, err := n.fs.snap.Lstat(p)
	if err != nil {
		if _, ok := err.(snapshot.PathError); ok {
			return nil, fuse.ENOENT
		}
		return nil, wrapError(err)
	}

	// Construct attr for the new child node.
	// First fill out the fields that are true for every file
	attr := makeAttr(fi, n.fs.ownerUID, n.fs.ownerGID, n.fs.blockSize)
	return &minNode{n.fs, p, attr}, nil
}

func (n *minNode) Open() (fs.Handle, error) {
	if Trace {
		log.Info("Min: Open entry ", n.p)
		defer log.Info("Min: Open exit ", n.p)
	}
	return &minHandle{n.fs, n.p}, nil
}

type minHandle struct {
	fs *slimMinFS
	p  string
}

func (h *minHandle) Release() error {
	return nil
}

func (h *minHandle) ReadAt(data []byte, offset int64) (int, error) {
	if Trace {
		log.Info("Min: Read entry ", h.p)
		defer log.Info("Min: Read exit ", h.p)
	}
	//TODO: snap.Open() should be called from minNode.Open() instead.
	f, err := h.fs.snap.Open(h.p)
	if err != nil {
		return 0, wrapError(err)
	}
	n, err := f.ReadAt(data, offset)
	f.Close() // don't defer since that creates a hot spot
	return n, wrapError(err)
}

func (h *minHandle) ReadDirAll() ([]fuse.Dirent, error) {
	if Trace {
		log.Info("Min: ReadDirAll entry ", h.p)
		defer log.Info("Min: ReadDirAll exit ", h.p)
	}
	dirents, err := h.fs.snap.Readdirents(h.p)
	if err != nil {
		return nil, wrapError(err)
	}
	r := make([]fuse.Dirent, len(dirents))
	for idx, d := range dirents {
		r[idx].Name = d.Name
		r[idx].Type = fuse.DirentType(int(d.Type))
	}
	return r, nil
}

func makeAttr(fi snapshot.FileInfo, ownerUID int, ownerGID int, blockSize uint32) fuse.Attr {
	var attr fuse.Attr
	attr.Uid = uint32(ownerUID)
	attr.Gid = uint32(ownerGID)
	attr.BlockSize = blockSize
	attr.Valid = 1 * time.Hour

	// Now per-file fields
	attr.Size = uint64(fi.Size())
	attr.Mode = 0444
	if fi.IsExec() || fi.IsDir() {
		attr.Mode |= 0111
	}

	switch fi.Type() {
	// No FT_File because that's 0.
	case snapshot.FT_Directory:
		attr.Mode |= os.ModeDir
	case snapshot.FT_Symlink:
		attr.Mode |= os.ModeSymlink
	}
	// TODO(dbentley): we could set links accurately by
	// extending snapshot.FileInfo to include NLink
	attr.Nlink = 1
	return attr
}

type _WrappedErr struct {
	errno      fuse.Errno
	underlying error
}

var _ fuse.ErrorNumber = (*_WrappedErr)(nil)

func (e *_WrappedErr) Errno() fuse.Errno { return e.errno }
func (e *_WrappedErr) Error() string     { return e.underlying.Error() }

func wrapError(err error) error {
	var errno syscall.Errno

	if _, ok := err.(*_WrappedErr); ok { // We already did this, just return it
		return err
	} else if e, ok := err.(*os.PathError); ok {
		errno = e.Err.(syscall.Errno)
	} else if e, ok := err.(*os.LinkError); ok {
		errno = e.Err.(syscall.Errno)
	} else if e, ok := err.(syscall.Errno); ok {
		errno = e
	} else if err == io.EOF {
		return nil
	} else {
		return err // not something we can wrap
	}

	return &_WrappedErr{
		errno:      fuse.Errno(errno),
		underlying: err,
	}
}
