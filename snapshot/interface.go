// Package snapshot provides interfaces and implementations for Scoot
// snapshots, which represent immutable filesystem state. This includes
// concepts for Files, and various sim/test implementations.
package snapshot

import "syscall"

type Snapshots interface {
	// Get the Snapshot identified by id. If no such snapshot exists,
	// the error will be a NoSuchSnapshotError.
	Get(id string) (Snapshot, error)
}

// A read-only, immutable Snapshot of filesystem state
type Snapshot interface {
	// The identifier of this Snapshot
	// Should be opaque to the client (they are handed IDs from somewhere and hand them to Snapshots).
	// Implementations might have IDs like git-<sha1 from git> or dbentley-newfeatures-12345
	// TODO(dbentley): namespaces for different implementations
	Id() string

	// Lstat returns a FileInfo describing the named file in the snapshot.
	// If the file is a symbolic link, the FileInfo describes the symbolic link.
	// LStat makes no attempt to follow the link. If there is an error because
	// the file doesn't exist it will be a PathError
	Lstat(name string) (FileInfo, error)

	// Stat returns a FileInfo describing the named file in the snapshot.
	// If the file is a symbolic link, the FileInfo describes destination
	// of the link. If there is an error because
	// the file doesn't exist in the snapshot, it will be a PathError
	Stat(name string) (FileInfo, error)

	// Readdirents reads the dirents of the named directory. If there is an error,
	// it will be a PathError.
	Readdirents(name string) ([]Dirent, error)

	// Readlink reads the destination of the named symbolic link.
	// If there is an error, it will be a PathError
	// If the named file is not a symbolic link, it will be a PathError
	Readlink(name string) (string, error)

	// Opens the named file. This is similar to os.Open, but an implementation
	// is allowed to defer checking if the file exists until the first
	// operation. If the file doesn't exist and the implementation checks,
	// the error will be a PathError.
	Open(path string) (File, error)
}

// File represents a file open for reading. An implementation of Snapshots
// may not keep a file open.
type File interface {
	// ReadAt reads len(b) bytes from the File starting at byte offset off.
	// It returns the number of bytes read and the error, if any.
	// ReadAt always returns a non-nil error when n < len(b).
	// At end of file, that error is io.EOF.
	ReadAt(p []byte, off int64) (int, error)

	// Read entire file, returning error for anything other than a complete read.
	// We define this rather than rely on ioutil.ReadAll to avoid coalescing small chunked reads.
	ReadAll() ([]byte, error)

	// Close closes the file, rendering it unusable for I/O.
	Close() error

	// TODO(dbentley): do we need the calls below? Let's not add them yet.
	// Path() string
	// Snapshot() Snapshot
}

// FileInfo represents the stat of a file in a Snapshot.
// It has less info that os.FileInfo.
type FileInfo interface {
	Type() FileType
	IsExec() bool
	Size() int64
	IsDir() bool
}

// Dirent represents a file as stored in a directory. This is less info
// than is stored in a file, and so has less info than a stat returns.
type Dirent struct {
	Name string
	Type FileType
	// TODO(dbentley): inode?
}

type FileType int

const (
	// We only support the types we want to support. Anything else will be Unknown.
	FT_Unknown   FileType = 0
	FT_Directory          = 4
	FT_File               = 8
	FT_Symlink            = 12
)

// TODO(dbentley): os returns PathError for many things in ways
// that are unspecified, so it's hard for us to be more exact, because
// os might change underneath us.
type PathError interface {
	PathError()
	Error() string
}

type DefaultPathError struct{}

func (err *DefaultPathError) PathError() {}
func (err *DefaultPathError) Error() string {
	return "PathError"
}
func (err *DefaultPathError) Errno() syscall.Errno {
	return syscall.ENOENT
}

type NoSuchSnapshotError interface {
	NoSuchSnapshotError()
	Error() string
}

type DefaultNoSuchSnapshotError struct{}

func (err *DefaultNoSuchSnapshotError) NoSuchSnapshotError() {}
func (err *DefaultNoSuchSnapshotError) Error() string {
	return "NoSuchSnapshotError"
}
func (err *DefaultNoSuchSnapshotError) Errno() syscall.Errno {
	return syscall.ENOENT
}

type FileCursor interface {
	// Read reads up to len(b) bytes from the File. It returns the number of
	// bytes read and an error, if any. EOF is signaled by a zero count
	// with err set to io.EOF.
	// Read requires per-file state; an implementation is (for now, during
	// development) allowed to panic on a call to Read if it does not wish
	// to maintain that state.
	Read(p []byte) (n int, err error)

	// ReadAt reads len(b) bytes from the File starting at byte offset off.
	// It returns the number of bytes read and the error, if any.
	// ReadAt always returns a non-nil error when n < len(b).
	// At end of file, that error is io.EOF.
	ReadAt(p []byte, off int64) (n int, err error)

	// Close closes the file, rendering it unusable for I/O.
	Close() error

	// TODO(dbentley): do we need the calls below? Let's not add them yet.
	// Path() string
	// Snapshot() Snapshot
}
