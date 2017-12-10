package bazel

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/os/temp"
)

var noopBf = bzFiler{
	command: noopBzCommand{},
}

const (
	defaultSha = "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
)

// Checkout tests

func TestValidBzCheckout(t *testing.T) {
	bc := &bzCheckout{}
	bc.Hash = defaultSha
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
	size := int64(5)
	id := generateId(defaultSha, size)
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
	size := int64(5)
	id := generateId(defaultSha, size)
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

func TestBzIngesteValidIngestDir(t *testing.T) {
	tmp, err := temp.TempDirDefault()
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	_, err = noopBf.Ingest(tmp.Dir)
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
	_, err = noopBf.Ingest(tmpFile.Name())
	if err != nil {
		t.Fatalf("Error ingesting file %v. Err: %v", tmpFile.Name(), err)
	}
	// make sure id checks out

	err = os.Remove(tmpFile.Name())
	if err != nil {
		t.Fatalf("Error removing %v: %v", tmpFile.Name(), err)
	}
}

func TestBzIngesterInvalidIngest(t *testing.T) {
	bf := bzFiler{
		command: bzCommand{
			command: "echo",
		},
	}
	_, err := bf.Ingest("some made up directory")
	if err == nil || !strings.Contains(err.Error(), noSuchFileOrDirMsg) {
		t.Fatalf("Expected error to contain %s, was %v", noSuchFileOrDirMsg, err)
	}
}

// Utils tests

func TestGetFileTypeDir(t *testing.T) {
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

func TestGetFileTypeFile(t *testing.T) {
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

func TestGetFileTypeInvalid(t *testing.T) {
	_, err := getFileType("some made up file or directory")
	if err == nil || !strings.Contains(err.Error(), noSuchFileOrDirMsg) {
		t.Fatalf("Expected error to contain %s, was %v", noSuchFileOrDirMsg, err)
	}
}

func TestValidateIdValid(t *testing.T) {
	size := int64(5)
	id := generateId(defaultSha, size)
	err := validateID(id)
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateIdInvalid(t *testing.T) {
	sha := "this/is/totally/wrong"
	size := int64(5)
	id := generateId(sha, size)
	err := validateID(id)
	if err == nil {
		t.Fatalf("Expected id %s to be invalid", id)
	}
	err = validateID(fmt.Sprintf("bs-%s-%d", defaultSha, size))
	if err == nil {
		t.Fatalf("Expected id %s to be invalid", id)
	}
}

func TestGetSha(t *testing.T) {
	size := int64(5)
	id := generateId(defaultSha, size)
	result, err := getSha(id)
	if err != nil {
		t.Fatal(err)
	}
	if result != defaultSha {
		t.Fatalf("Expected %s, got %s", defaultSha, result)
	}
}

func TestGetSize(t *testing.T) {
	size := int64(5)
	id := generateId(defaultSha, size)
	result, err := getSize(id)
	if err != nil {
		t.Fatal(err)
	}
	if result != size {
		t.Fatalf("Expected %d, got %d", size, result)
	}
}

func TestSplitIdValid(t *testing.T) {
	size := int64(5)
	id := generateId(defaultSha, size)
	result, err := splitId(id)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{bzSnapshotIdPrefix, defaultSha, "5"}
	for idx, _ := range result {
		if result[idx] != expected[idx] {
			t.Fatalf("Expected %v, received %v", expected, result)
		}
	}
}

func TestSplitIdInvalid(t *testing.T) {
	size := int64(5)
	id := fmt.Sprintf("bs-%s-%d", defaultSha, size)
	_, err := splitId(id)
	if err == nil || !strings.Contains(err.Error(), invalidIdMsg) {
		t.Fatalf("Expected error to contain \"%s\", received \"%v\"", invalidIdMsg, err)
	}
}

// TODO: integration tests with CAS once we have fs_util binary
