package snapshots

import (
	"path/filepath"
	"sort"

	"github.com/twitter/scoot/snapshot"
)

type FakeFile interface {
	IsExec() bool
	Size() int64

	Type() snapshot.FileType
	IsDir() bool
}

func NewDir(children map[string]FakeFile) *FakeDir {
	return &FakeDir{children}
}

type FakeDir struct {
	Children map[string]FakeFile
}

func (f *FakeDir) IsExec() bool {
	// Directories are executable
	return true
}

func (f *FakeDir) Size() int64 {
	return int64(len(f.Children))
}

func (f *FakeDir) Type() snapshot.FileType {
	return snapshot.FT_Directory
}

func (f *FakeDir) IsDir() bool {
	return true
}

func NewContents(contents string, exec bool) *FakeContents {
	return &FakeContents{contents, exec}
}

type FakeContents struct {
	Contents string
	Exec     bool
}

func (f *FakeContents) IsExec() bool {
	return f.Exec
}

func (f *FakeContents) Size() int64 {
	return int64(len(f.Contents))
}

func (f *FakeContents) Type() snapshot.FileType {
	return snapshot.FT_File
}

func (f *FakeContents) IsDir() bool {
	return false
}

func (f *FakeContents) ReadAt(p []byte, off int64) (int, error) {
	// TODO(dbentley): return EOF at end of file
	data := window([]byte(f.Contents), off, len(p))
	n := copy(p, data)
	return n, nil
}

func (f *FakeContents) ReadAll() ([]byte, error) {
	return []byte(f.Contents), nil
}

func (f *FakeContents) Close() error {
	return nil
}

func NewSnapshot(root *FakeDir, id string) *FakeSnapshot {
	return &FakeSnapshot{root, id}
}

type FakeSnapshot struct {
	Root *FakeDir
	ID   string
}

func (s *FakeSnapshot) Id() string {
	return s.ID
}

// Find file resolves a Snapshot ID and path pair into a file (or an error)
func (s *FakeSnapshot) findFile(path string) (FakeFile, error) {
	return findFile(s.Root, path)
}

// Split a path a/b/c into a and b/c
func splitFirst(path string) (car string, cdr string) {
	return splitFirstHelper(path, "")
}

// Recursive helper function to split the string.
func splitFirstHelper(dir string, rest string) (string, string) {
	if dir == "/" {
		return dir, rest
	}
	d := filepath.Dir(dir)
	if d == "" || d == "." {
		return dir, rest
	}
	rest = filepath.Join(filepath.Base(dir), rest)
	if d == "/" {
		return "/", rest
	}
	return splitFirstHelper(d, rest)
}

// TODO(dbentley): support symlinks
// TODO(dbentley): have a version of findFile that follows symlinks

// Recursively navigate through a directory structure to find `path` within `file`.
func findFile(file FakeFile, path string) (FakeFile, error) {
	if path == "" {
		return file, nil
	}
	switch f := file.(type) {
	case *FakeContents:
		return nil, &pathError{}
	case *FakeDir:
		dirName, rest := splitFirst(path)
		child := f.Children[dirName]
		if child == nil {
			return nil, &pathError{}
		}
		return findFile(child, rest)
	default:
		panic("Inconceivable")

	}
}

type pathError struct {
}

func (e *pathError) Error() string {
	return ""
}

func (s *FakeSnapshot) Lstat(name string) (snapshot.FileInfo, error) {
	return findFile(s.Root, name)
}

func (s *FakeSnapshot) Stat(name string) (snapshot.FileInfo, error) {
	return findFile(s.Root, name)
}

func (s *FakeSnapshot) Readdirents(name string) ([]snapshot.Dirent, error) {
	f, err := s.findFile(name)
	if err != nil {
		return nil, err
	}
	switch f := f.(type) {
	case *FakeContents:
		return nil, &pathError{}
	case *FakeDir:
		r := make([]snapshot.Dirent, len(f.Children))
		keys := make([]string, len(f.Children))
		i := 0
		for k, _ := range f.Children {
			keys[i] = k
			i++
		}
		sort.Strings(keys)
		for idx, name := range keys {
			r[idx] = toDirent(f.Children[name], name)
		}
		return r, nil
	default:
		panic("inconceivable")
	}

}

func toDirent(f FakeFile, name string) snapshot.Dirent {
	result := snapshot.Dirent{}
	result.Name = name
	switch f.(type) {
	case *FakeContents:
		result.Type = snapshot.FT_File
	case *FakeDir:
		result.Type = snapshot.FT_Directory
	}
	return result
}

func (s *FakeSnapshot) Readlink(name string) (string, error) {
	// TODO(dbentley): support symlinks
	return "", &pathError{}
}

func (s *FakeSnapshot) Open(path string) (snapshot.File, error) {
	f, err := s.findFile(path)
	if err != nil {
		return nil, err
	}

	switch f := f.(type) {
	case *FakeContents:
		return f, nil
	default:
		// TODO(dbentley): should probably do something better
		panic(f)
	}
}

// TODO(dbentley): move somewhere common; very similar to scoot/fs/min/servlet.go:window
func window(data []byte, offset int64, size int) []byte {
	if offset >= int64(len(data)) {
		return nil
	}
	data = data[offset:]
	if len(data) > size {
		data = data[:size]
	}
	return data
}
