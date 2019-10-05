package tree

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/twitter/scoot/bazel"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	scootproto "github.com/twitter/scoot/common/proto"
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

func TestReadDir(t *testing.T) {
	tmp, err := ioutil.TempDir("", "tree-test")
	if err != nil {
		t.Fatalf("Error setting up tempdir: %s", err)
	}
	defer os.RemoveAll(tmp)

	// write dir containing known sha sum contents with the following:
	// regular file
	// executable file
	// 2x subdir
	// 2x symlink across dirs

	rf := filepath.Join(tmp, "reg")
	if err := ioutil.WriteFile(rf, []byte("aaa"), 0600); err != nil {
		t.Fatalf("Failed to write file: %s", err)
	}
	ef := filepath.Join(tmp, "exec")
	if err := ioutil.WriteFile(ef, []byte("zzz"), 0755); err != nil {
		t.Fatalf("Failed to write file: %s", err)
	}
	if err := os.Mkdir(filepath.Join(tmp, "subdir"), 0777); err != nil {
		t.Fatalf("Failed to make dir: %s", err)
	}
	if err := os.Mkdir(filepath.Join(tmp, "subdir2"), 0777); err != nil {
		t.Fatalf("Failed to make dir: %s", err)
	}
	rf2 := filepath.Join(tmp, "subdir2", "reg2")
	if err := ioutil.WriteFile(rf2, []byte("xxx"), 0644); err != nil {
		t.Fatalf("Failed to write file: %s", err)
	}
	// chdir to set relative symlinks - must be from the dir they appear in
	if err := os.Chdir(filepath.Join(tmp, "subdir")); err != nil {
		t.Fatalf("Failed to chdir: %s", err)
	}
	if err := os.Symlink(filepath.Join("..", "exec"), "sl"); err != nil {
		t.Fatalf("Failed to make symlink: %s", err)
	}
	if err := os.Symlink(filepath.Join("..", "reg"), "reg"); err != nil {
		t.Fatalf("Failed to make symlink: %s", err)
	}
	if err := os.Chdir(filepath.Join(tmp, "subdir2")); err != nil {
		t.Fatalf("Failed to chdir: %s", err)
	}
	if err := os.Symlink("reg2", "sl2"); err != nil {
		t.Fatalf("Failed to make symlink: %s", err)
	}

	d, err := ReadDirectory(tmp)
	if err != nil {
		t.Fatalf("Failed to read dir: %s", err)
	}

	// TODO comments, readability, etc

	subdir := &remoteexecution.Directory{
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "reg",
				Target: "../reg",
			},
			{
				Name:   "sl",
				Target: "../exec",
			},
		},
		Directories: []*remoteexecution.DirectoryNode{},
		Files:       []*remoteexecution.FileNode{},
	}
	subdir2 := &remoteexecution.Directory{
		Symlinks: []*remoteexecution.SymlinkNode{
			{
				Name:   "sl2",
				Target: "reg2",
			},
		},
		Directories: []*remoteexecution.DirectoryNode{},
		Files: []*remoteexecution.FileNode{
			{
				Name: "reg2",
				Digest: &remoteexecution.Digest{
					Hash:      "cd2eb0837c9b4c962c22d2ff8b5441b7b45805887f051d39bf133b583baf6860",
					SizeBytes: 3,
				},
				IsExecutable: false,
			},
		},
	}
	subdirhash, subdirsize, err := scootproto.GetSha256(subdir)
	if err != nil {
		t.Fatalf("Failed to compute expected Sha256: %s", err)
	}
	subdir2hash, subdir2size, err := scootproto.GetSha256(subdir2)
	if err != nil {
		t.Fatalf("Failed to compute expected Sha256: %s", err)
	}
	dir := &remoteexecution.Directory{
		Symlinks: []*remoteexecution.SymlinkNode{},
		Directories: []*remoteexecution.DirectoryNode{
			{
				Name:   "subdir",
				Digest: &remoteexecution.Digest{Hash: subdirhash, SizeBytes: subdirsize},
			},
			{
				Name:   "subdir2",
				Digest: &remoteexecution.Digest{Hash: subdir2hash, SizeBytes: subdir2size},
			},
		},
		Files: []*remoteexecution.FileNode{
			{
				Name: "exec",
				Digest: &remoteexecution.Digest{
					Hash:      "17f165d5a5ba695f27c023a83aa2b3463e23810e360b7517127e90161eebabda",
					SizeBytes: 3,
				},
				IsExecutable: true,
			},
			{
				Name: "reg",
				Digest: &remoteexecution.Digest{
					Hash:      "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0",
					SizeBytes: 3,
				},
				IsExecutable: false,
			},
		},
	}

	hash, size, err := scootproto.GetSha256(dir)
	if err != nil {
		t.Fatalf("Failed to compute expected Sha256: %s", err)
	}
	e := &remoteexecution.Digest{Hash: hash, SizeBytes: size}

	if !bazel.DigestsEqual(d, e) {
		t.Fatalf("Digest from read dir didn't match; got: %s, want: %s", d, e)
	}
}
