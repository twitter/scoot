package tree

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/twitter/scoot/bazel"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
)

var (
	testHash1 string = "36f583dd16f4e1e201eb1e6f6d8e35a2ccb3bbe2658de46b4ffae7b0e9ed872e"
	testSize1 int64  = 7
	testData1 []byte = []byte("abc1234")
)

func TestReadFile(t *testing.T) {
	tmp, err := ioutil.TempDir("", "tree-test")
	if err != nil {
		t.Fatalf("Error setting up tempdir: %s", err)
	}
	defer os.RemoveAll(tmp)

	fname := filepath.Join(tmp, "test.txt")
	if err := ioutil.WriteFile(fname, testData1, 0644); err != nil {
		t.Fatalf("Failed to write file: %s", err)
	}

	d, err := ReadFile(fname)
	if err != nil {
		t.Fatalf("Failed to read file: %s", err)
	}

	e := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	if !bazel.DigestsEqual(d, e) {
		t.Fatalf("Digest from read file didn't match; got: %s, want: %s", d, e)
	}
}

/*
// TODO requirements:
//	* regular file
//	* executable file
//	* subdir
//	* symlink across dirs
func TestReadDir(t *testing.T) {
	tmp, err := ioutil.TempDir("", "tree-test")
	if err != nil {
		t.Fatalf("Error setting up tempdir: %s", err)
	}
	defer os.RemoveAll(tmp)

	td := "/Users/dgassaway/workspace/bazel/dir"
	d, err := ReadDirectory(td)
	if err != nil {
		t.Fatalf("Failed to read dir: %s", err)
	}

	e := &remoteexecution.Digest{Hash: "ff05536f4eff3eaef6062023d560f6e1b131d7123690606b2748e2a6f4454637", SizeBytes: 321}
	if !bazel.DigestsEqual(d, e) {
		t.Fatalf("Digest from read dir didn't match; got: %s, want: %s", d, e)
	}
}
*/
