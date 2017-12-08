package bazel

import (
	"os"
	"strings"
	"testing"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/os/temp"
)

var noopBf = bzFiler{command: "echo"}

// Checkout tests

func TestValidBzCheckout(t *testing.T) {
	bc := &bzCheckout{}
	bc.Hash = "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	bc.SizeBytes = int64(10)
	if !bazel.IsValidDigest(bc.GetHash(), bc.GetSizeBytes()) {
		t.Fatalf("Expected valid hash and size")
	}
}

func TestInvalidBzCheckout(t *testing.T) {
	bc := &bzCheckout{}
	bc.Hash = "this can't be right"
	bc.SizeBytes = int64(10)
	if bazel.IsValidDigest(bc.ID(), bc.GetSizeBytes()) {
		t.Fatalf("Expected invalid hash")
	}
}

func TestReleaseBzCheckout(t *testing.T) {
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	bc := &bzCheckout{dir: tempDir.Dir}
	bc.Release()
	_, err = os.Stat(tempDir.Dir)
	if !os.IsNotExist(err) {
		t.Fatalf("Directory was not successfully removed. %v", err)
	}
}

// Checkouter tests
func TestBzCheckouterInvalidCheckout(t *testing.T) {
	_, err := noopBf.Checkout("this can't be right either")
	if err == nil || !strings.Contains(err.Error(), invalidIdMsg) {
		t.Fatalf("Expected checkout to be invalid due to ID")
	}
}

func TestBzCheckouterValidCheckout(t *testing.T) {
	sha := "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	size := int64(5)
	id := generateId(sha, size)
	snap, err := noopBf.Checkout(id)
	if err != nil {
		t.Fatalf("Expected checkout to be valid. Err: %v", err)
	}
	if snap.ID() != id {
		t.Fatalf("Expected snapshot ID to be %v, was %v", id, snap.ID())
	}
}

func TestBzCheckouterInvalidCheckoutAt(t *testing.T) {
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	_, err = noopBf.CheckoutAt("this definitely isn't right", tempDir.Dir)
	if err == nil || !strings.Contains(err.Error(), invalidIdMsg) {
		t.Fatalf("Expected checkout to be invalid due to ID")
	}
}

func TestBzCheckouterValidCheckoutAt(t *testing.T) {
	sha := "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	size := int64(5)
	id := generateId(sha, size)
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	snap, err := noopBf.CheckoutAt(id, tempDir.Dir)
	if err != nil {
		t.Fatalf("Expected checkout to be valid. Err: %v", err)
	}
	if snap.ID() != id {
		t.Fatalf("Expected snapshot ID to be %v, was %v", id, snap.ID())
	}
}

// Ingester tests

func TestBzIngesterGetFileTypeDir(t *testing.T) {
	tmp, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}

	fileType, err := getFileType(tmp.Dir)
	if err != nil || fileType != fsUtilCmdDirectory {
		t.Fatalf("Expected fileType to be %s, was %s. Err: %v", fsUtilCmdDirectory, fileType, err)
	}

	err = os.Remove(tmp.Dir)
	if err != nil {
		t.Fatalf("Error removing %v: %v", tmp.Dir, err)
	}
}

func TestBzIngesterGetFileTypeFile(t *testing.T) {
	tmp, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}

	tmpFile, err := tmp.TempFile("")
	if err != nil {
		t.Fatalf("Error creating temp file. %v", err)
	}

	fileType, err := getFileType(tmpFile.Name())
	if err != nil || fileType != fsUtilCmdFile {
		t.Fatalf("Expected fileType to be %s, was %s. Err: %v", fsUtilCmdFile, fileType, err)
	}

	err = os.Remove(tmpFile.Name())
	if err != nil {
		t.Fatalf("Error removing %v: %v", tmpFile.Name(), err)
	}
}

func TestBzIngesterGetFileTypeInvalid(t *testing.T) {
	_, err := getFileType("some made up file or directory")
	if err == nil || !strings.Contains(err.Error(), noSuchFileOrDirMsg) {
		t.Fatalf("Expected error to contain %s, was %v", noSuchFileOrDirMsg, err)
	}
}

func TestBzIngesterValidIngestDir(t *testing.T) {
	tmp, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	fn := func(s string) error { return nil }
	_, err = noopBf.IngestAndValidate(tmp.Dir, validator(fn))
	if err != nil {
		t.Fatalf("Error ingesting dir %v. Err: %v", tmp.Dir, err)
	}

	err = os.Remove(tmp.Dir)
	if err != nil {
		t.Fatalf("Error removing %v: %v", tmp.Dir, err)
	}
}

func TestBzIngesterValidIngestFile(t *testing.T) {
	tmp, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}

	tmpFile, err := tmp.TempFile("")
	if err != nil {
		t.Fatalf("Error creating temp file. %v", err)
	}
	fn := func(s string) error { return nil }
	_, err = noopBf.IngestAndValidate(tmpFile.Name(), validator(fn))
	if err != nil {
		t.Fatalf("Error ingesting file %v. Err: %v", tmpFile.Name(), err)
	}

	err = os.Remove(tmpFile.Name())
	if err != nil {
		t.Fatalf("Error removing %v: %v", tmpFile.Name(), err)
	}
}

func TestBzIngesterInvalidIngest(t *testing.T) {
	_, err := noopBf.Ingest("some made up directory")
	if err == nil || !strings.Contains(err.Error(), noSuchFileOrDirMsg) {
		t.Fatalf("Expected error to contain %s, was %v", noSuchFileOrDirMsg, err)
	}
}

// TODO: integration tests with CAS once we have fs_util binary
