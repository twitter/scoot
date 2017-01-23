package bundlestore

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/scootdev/scoot/os/temp"
)

func TestBrowseStore(t *testing.T) {
	tmp, _ := temp.NewTempDir("", "testBrowseStore")
	defer os.RemoveAll(tmp.Dir)

	// Create a bundle and make it available to fileStore.
	fileStore, _ := MakeFileStore(tmp.Dir)
	bundleName := "a-b-c"

	srcTmp, _ := tmp.TempDir("src")
	str := "git init; echo -n foo > bar; git add bar; git commit -m'First'; git bundle create %s/%s master"
	cmd := exec.Command("bash", "-c", fmt.Sprintf(str, tmp.Dir, bundleName))
	cmd.Dir = srcTmp.Dir
	if err := cmd.Run(); err != nil {
		t.Fatalf(err.Error())
	}

	// Request bundle content from the browse store.
	store := MakeCachingBrowseStore(fileStore, tmp)
	if reader, err := store.OpenForRead(filepath.Join(bundleName, "bar")); err != nil {
		t.Fatalf(err.Error())
	} else if data, err := ioutil.ReadAll(reader); err != nil {
		t.Fatalf(err.Error())
	} else if string(data) != "foo" {
		t.Fatalf("Expected 'foo', got: '%s'", string(data))
	}
}
