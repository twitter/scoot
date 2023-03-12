package cleaner

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wisechengyi/scoot/cleaner/dirconfig"
)

func TestCleanupBadConfig(t *testing.T) {
	// expect error - LowMark == HighMark
	_, err := dirconfig.NewLastModifiedDirConfig("/this/dir/does/not/exist", 0, 0, 0, 0)
	if err == nil {
		t.Fatal("Expected error making new cleaner where LowMarkKB == HighMarkKB == 0")
	}
}

func TestCleanupFakeDir(t *testing.T) {
	// expected to work without errors although Dir doesn't exist
	dirConf, err := dirconfig.NewLastModifiedDirConfig("/this/dir/does/not/exist", 0, 0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to create DirConfig: %s", err)
	}
	dc, err := NewDiskCleaner([]dirconfig.DirConfig{dirConf})
	if err != nil {
		t.Fatalf("Failed to make new cleaner: %s", err)
	}

	if err = dc.Cleanup(); err != nil {
		t.Fatalf("Unexpected error cleaning up non-existent dir: %s", err)
	}
}

func TestCleanupAtWatermarks(t *testing.T) {
	tmp, err := ioutil.TempDir("", "cleaner-test")
	if err != nil {
		t.Fatalf("Error setting up tempdir: %s", err)
	}
	defer os.RemoveAll(tmp)

	dirConf, err := dirconfig.NewLastModifiedDirConfig(tmp, 64, 2880, 128, 1440)
	dc, err := NewDiskCleaner([]dirconfig.DirConfig{dirConf})
	if err != nil {
		t.Fatalf("Failed to make new cleaner: %s", err)
	}

	// write files and cleanup:
	// f0: post retention
	// size < low mark
	// expected not to be deleted
	f0 := filepath.Join(tmp, "f0")
	makeFile(t, f0, 0, time.Now().Add(time.Duration(-49)*time.Hour))
	dc.Cleanup()
	if _, err = os.Stat(f0); os.IsNotExist(err) {
		t.Fatalf("File %s unexpectedly cleaned up", f0)
	}

	// write files and cleanup:
	// f1: post retention
	// f2: pre retention
	// combined >= low mark, <= high mark
	// expected f1 is deleted
	f1 := filepath.Join(tmp, "f1")
	f2 := filepath.Join(tmp, "f2")
	makeFile(t, f1, 32, time.Now().Add(time.Duration(-49)*time.Hour))
	makeFile(t, f2, 32, time.Now().Add(time.Duration(-25)*time.Hour))
	dc.Cleanup()
	if _, err = os.Stat(f1); !os.IsNotExist(err) {
		t.Fatalf("File %s exists, expected to be cleaned up", f1)
	}
	if _, err = os.Stat(f2); os.IsNotExist(err) {
		t.Fatalf("File %s unexpectedly cleaned up", f2)
	}

	// write files and cleanup:
	// f3: post retention
	// f4: pre retention
	// combined >= high mark
	// expected f3 is deleted
	f3 := filepath.Join(tmp, "f3")
	f4 := filepath.Join(tmp, "f4")
	makeFile(t, f3, 64, time.Now().Add(time.Duration(-25)*time.Hour))
	makeFile(t, f4, 64, time.Now().Add(time.Duration(-12)*time.Hour))
	dc.Cleanup()
	if _, err = os.Stat(f3); !os.IsNotExist(err) {
		t.Fatalf("File %s exists, expected to be cleaned up", f3)
	}
	if _, err = os.Stat(f4); os.IsNotExist(err) {
		t.Fatalf("File %s unexpectedly cleaned up", f4)
	}
}

func TestCleanupEntireDir(t *testing.T) {
	tmp, err := ioutil.TempDir("", "cleaner-test")
	if err != nil {
		t.Fatalf("Error setting up tempdir: %s", err)
	}
	defer os.RemoveAll(tmp)

	dirConf := []dirconfig.DirConfig{
		dirconfig.EntireDirConfig{
			Dir:        tmp,
			MaxUsageKB: 128,
		},
	}
	dc, err := NewDiskCleaner(dirConf)
	if err != nil {
		t.Fatalf("Failed to make new cleaner: %s", err)
	}

	f0 := filepath.Join(tmp, "f0")
	makeFile(t, f0, 100, time.Now())
	dc.Cleanup()
	if _, err = os.Stat(f0); os.IsNotExist(err) {
		t.Fatalf("File %s unexpectedly cleaned up", f0)
	}
	if _, err = os.Stat(tmp); os.IsNotExist(err) {
		t.Fatalf("Dir %s exists, expected to be cleaned up", tmp)
	}

	f1 := filepath.Join(tmp, "f1")
	makeFile(t, f1, 100, time.Now())
	dc.Cleanup()
	if _, err = os.Stat(f1); !os.IsNotExist(err) {
		t.Fatalf("File %s exists, expected to be cleaned up", f1)
	}
	if _, err = os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("Dir %s exists, expected to be cleaned up", tmp)
	}
}

func TestCleanupEntireFakeDir(t *testing.T) {
	dirConf := dirconfig.EntireDirConfig{
		Dir:        "/this/dir/does/not/exist",
		MaxUsageKB: 0,
	}
	dc, err := NewDiskCleaner([]dirconfig.DirConfig{dirConf})
	if err != nil {
		t.Fatalf("Failed to make new cleaner: %s", err)
	}
	if err := dc.Cleanup(); err != nil {
		t.Fatalf("Unexpected error cleaning up non-existent dir: %s", err)
	}
}

func makeFile(t *testing.T, path string, sizeKB uint64, lastAccess time.Time) {
	buffer := make([]byte, sizeKB*1024)
	rand.Read(buffer)

	if err := ioutil.WriteFile(path, buffer, 0644); err != nil {
		t.Fatalf("Failed to generate file: %s", err)
	}

	if err := os.Chtimes(path, lastAccess, lastAccess); err != nil {
		t.Fatalf("Failed to chtime on file: %s", err)
	}
}
