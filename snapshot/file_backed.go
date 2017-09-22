package snapshot

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"

	"github.com/twitter/scoot/fs/perf"
)

var Trace bool

// An implementation of the Snapshots interface backed by a filesystem.

type fileBackedSnapshots struct {
	root string
}

func NewFileBackedSnapshots(root string) Snapshots {
	return &fileBackedSnapshots{root}
}

func NewFileBackedSnapshot(root string, id string) Snapshot {
	return &fileBackedSnapshot{root, id}
}

func (s *fileBackedSnapshots) Get(id string) (Snapshot, error) {
	return &fileBackedSnapshot{s.root, id}, nil
}

type fileBackedSnapshot struct {
	root string
	id   string
}

func (s *fileBackedSnapshot) Id() string {
	return s.id
}

func (s *fileBackedSnapshot) Lstat(name string) (FileInfo, error) {
	fi, err := os.Lstat(perf.UnsafePathJoin(true, s.root, name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &pathError{err}
		}
		return nil, err
	}
	return &fileBackedFileInfo{fi}, nil
}

func (s *fileBackedSnapshot) Stat(name string) (FileInfo, error) {
	fi, err := os.Stat(perf.UnsafePathJoin(true, s.root, name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &pathError{err}
		}
		return nil, err
	}
	return &fileBackedFileInfo{fi}, nil

}

type fileBackedFileInfo struct {
	fi os.FileInfo
}

func (i *fileBackedFileInfo) Type() FileType {
	switch i.fi.Mode() & os.ModeType {
	case 0:
		return FT_File
	case os.ModeDir:
		return FT_Directory
	case os.ModeSymlink:
		return FT_Symlink
	default:
		return FT_Unknown
	}
}

func (i *fileBackedFileInfo) Size() int64 {
	return i.fi.Size()
}

func (i *fileBackedFileInfo) IsDir() bool {
	return i.fi.IsDir()
}

func (i *fileBackedFileInfo) IsExec() bool {
	return (i.fi.Mode() & 0111) != 0
}

func (s *fileBackedSnapshot) Readdirents(name string) ([]Dirent, error) {
	// TODO(dbentley): handle directories too big to read in one go
	if Trace {
		log.Info("Snap: Readdirents entry ", name)
		defer log.Info("Snap: Readdirents exit ", name)
	}

	// TODO(dbentley): find a way to keep the file open instead of opening it here
	f, err := os.Open(perf.UnsafePathJoin(true, s.root, name))
	defer f.Close()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &pathError{err}
		}
		return nil, err
	}
	return readDirents(int(f.Fd()))
}

func (s *fileBackedSnapshot) Readlink(name string) (string, error) {
	r, err := os.Readlink(perf.UnsafePathJoin(true, s.root, name))
	if err != nil {
		return "", &pathError{err}
	}
	return r, nil
}

func (s *fileBackedSnapshot) Open(name string) (File, error) {
	// Flag that the joined string is not discarded because callers will almost certainly store it.
	f, err := os.Open(perf.UnsafePathJoin(false, s.root, name))
	if err != nil {
		return nil, &pathError{err}
	}
	return &fileBackedFile{f}, nil
}

type fileBackedFile struct {
	back *os.File
}

func (f *fileBackedFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.back.ReadAt(p, off)
}

func (f *fileBackedFile) ReadAll() ([]byte, error) {
	return ioutil.ReadFile(f.back.Name())
}

func (f *fileBackedFile) Close() error {
	return f.back.Close()
}

// TODO(dbentley): change to have File support a .ToCursor method.
func MakeCursor(f File) FileCursor {
	return &fileCursor{f, 0}
}

// A file cursor backed by a File. This adds state to a File
type fileCursor struct {
	back   File
	offset int64
}

func (f *fileCursor) Read(p []byte) (n int, err error) {
	n, err = f.back.ReadAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *fileCursor) ReadAt(p []byte, off int64) (n int, err error) {
	return f.back.ReadAt(p, off)
}

func (f *fileCursor) ReadAll() ([]byte, error) {
	return f.back.ReadAll()
}

func (f *fileCursor) Close() error {
	return f.back.Close()
}
