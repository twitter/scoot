package snapshots_test

import (
	"bytes"
	"testing"

	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

const (
	dir  = snapshot.FT_Directory
	file = snapshot.FT_File
)

func TestFakes(t *testing.T) {
	snap := snapshots.NewSnapshot(
		snapshots.NewDir(
			map[string]snapshots.FakeFile{
				"foo.txt": snapshots.NewContents("foo text", false),
				"foo.py":  snapshots.NewContents("foo code", true),
			},
		),
		"")

	assertDirents(
		[]snapshot.Dirent{
			{Name: "foo.py", Type: snapshot.FT_File},
			{Name: "foo.txt", Type: snapshot.FT_File},
		},
		nil, snap, "", t)
	assertStat(nil, dir, true, -1, snap, "", t)
	assertStat(nil, file, false, 8, snap, "foo.txt", t)
	assertStat(nil, file, true, 8, snap, "foo.py", t)

	f := assertOpen(nil, snap, "foo.py", t)
	assertReadAll([]byte("foo code"), nil, f, t)
	assertReadAt([]byte("oo cod"), nil, f, 1, 6, t)
}

func assertOpen(expectedErr error, snap snapshot.Snapshot, path string, t *testing.T) snapshot.File {
	f, err := snap.Open(path)
	if err != expectedErr {
		t.Fatalf("Unexpected err opening %v: %v (expected %v)", path, err, expectedErr)
	}
	return f
}

func assertReadAll(expected []byte, expectedErr error, f snapshot.File, t *testing.T) {
	data, err := f.ReadAll()
	if err != expectedErr {
		t.Fatalf("Unexpected err reading all: %v (expected %v)", err, expectedErr)
	}
	if err != nil {
		return
	}
	if !bytes.Equal(data, expected) {
		t.Fatalf("Unexpected data: %v (expected %v)", data, expected)
	}
}

func assertReadAt(expected []byte, expectedErr error, f snapshot.File, offset int64, l int, t *testing.T) {
	bs := make([]byte, l)
	n, err := f.ReadAt(bs, offset)
	if err != expectedErr {
		t.Fatalf("Unexpected err reading all: %v (expected %v)", err, expectedErr)
	}
	if err != nil {
		return
	}
	bs = bs[:n]
	if !bytes.Equal(bs, expected) {
		t.Fatalf("Unexpected data: %v (expected %v)", bs, expected)
	}
}
func assertStat(expectedErr error, expectedType snapshot.FileType, exec bool, size int64, snap snapshot.Snapshot, path string, t *testing.T) {
	fi, err := snap.Stat(path)
	if err != expectedErr {
		t.Fatalf("Unexpected error stat'ing %v: %v (expected %v)", path, err, expectedErr)
	}
	if err != nil {
		return
	}
	if expectedType != fi.Type() {
		t.Fatalf("Unexpected file type for %v: %v (expected %v)", path, fi.Type(), expectedType)
	}
	if fi.IsDir() != (expectedType == dir) {
		t.Fatalf("Unexpected IsDir for %v: %v (expected %v)", path, fi.IsDir(), expectedType == dir)
	}
	if fi.IsExec() != exec {
		t.Fatalf("Unexpected IsExec for %v: %v (expected %v)", path, fi.IsExec(), exec)
	}
	if size != -1 {
		if fi.Size() != size {
			t.Fatalf("Unexpected size for %v: %v (expected %v)", path, fi.Size(), size)
		}
	}
}

func assertDirents(expected []snapshot.Dirent, expectedErr error, snap snapshot.Snapshot, path string, t *testing.T) {
	actual, err := snap.Readdirents(path)
	if err != expectedErr {
		t.Fatalf("Unexpected error reading dirents for %v: %v (expected %v)", path, err, expectedErr)
	}
	if err != nil {
		return
	}
	if len(expected) != len(actual) {
		t.Fatalf("dirents unequal in length. expected: %v, got: %v", expected, actual)
	}

	for idx, e := range expected {
		a := actual[idx]
		if e != a {
			t.Fatalf("Non-matching dirents. %v %v (from %v %v)", e, a, expected, actual)
		}
	}
}
