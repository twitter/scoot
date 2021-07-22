package bazel

import (
	"fmt"
	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/log/hooks"
	"io/ioutil"
)

var noopBf *BzFiler
var tmpTest string
var noopRes = dialer.NewConstantResolver("")

func init() {
	log.AddHook(hooks.NewContextHook())
}

func setup() (string, *BzFiler) {
	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	bf := &BzFiler{
		tree: &noopBzTree{},
		tmp:  tmp,
	}
	return tmp, bf
}

func teardown(tmp string) {
	os.Remove(tmp)
}

func TestMain(m *testing.M) {
	tmpTest, noopBf = setup()
	rc := m.Run()
	teardown(tmpTest)
	os.Exit(rc)
}

// Checkout tests

func TestValidBzCheckout(t *testing.T) {
	bc := &bzCheckout{}
	bc.Hash = bazel.EmptySha
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
	tempDir, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	bc := &bzCheckout{dir: tempDir}
	bc.Release()
	_, err = os.Stat(tempDir)
	if !os.IsNotExist(err) {
		t.Fatalf("Directory was not successfully removed. %v", err)
	}
}

// Checkouter tests

func TestBzCheckouterInvalidCheckout(t *testing.T) {
	_, err := noopBf.Checkout("this can't be right either")
	if err == nil || !strings.Contains(err.Error(), bazel.InvalidIDMsg) {
		t.Fatalf("Expected checkout to be invalid due to ID")
	}
}

func TestBzCheckouterValidCheckout(t *testing.T) {
	size := int64(5)
	id := bazel.SnapshotID(bazel.EmptySha, size)
	snap, err := noopBf.Checkout(id)
	if err != nil {
		t.Fatalf("Expected checkout to be valid. Err: %v", err)
	}
	if snap.ID() != id {
		t.Fatalf("Expected snapshot ID to be %v, was %v", id, snap.ID())
	}
}

func TestBzCheckouterInvalidCheckoutAt(t *testing.T) {
	tempDir, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	_, err = noopBf.CheckoutAt("this definitely isn't right", tempDir)
	if err == nil || !strings.Contains(err.Error(), bazel.InvalidIDMsg) {
		t.Fatalf("Expected checkout to be invalid due to ID")
	}
}

func TestBzCheckouterValidCheckoutAt(t *testing.T) {
	size := int64(5)
	id := bazel.SnapshotID(bazel.EmptySha, size)
	tempDir, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	snap, err := noopBf.CheckoutAt(id, tempDir)
	if err != nil {
		t.Fatalf("Expected checkout to be valid. Err: %v", err)
	}
	if snap.ID() != id {
		t.Fatalf("Expected snapshot ID to be %v, was %v", id, snap.ID())
	}
}

// Ingester tests

func TestBzIngesteValidIngestDir(t *testing.T) {
	tmp, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	_, err = noopBf.Ingest(tmp)
	if err != nil {
		t.Fatalf("Error ingesting dir %v. Err: %v", tmp, err)
	}
}

func TestBzIngesterValidIngestFile(t *testing.T) {
	tmp, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}

	tmpFile, err := ioutil.TempFile(tmp, "")
	if err != nil {
		t.Fatalf("Error creating temp file. %v", err)
	}
	_, err = noopBf.Ingest(tmpFile.Name())
	if err != nil {
		t.Fatalf("Error ingesting file %v. Err: %v", tmpFile.Name(), err)
	}
	// make sure id checks out
}

func TestBzIngesterInvalidIngest(t *testing.T) {
	tmp, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}
	bf, err := MakeBzFiler(tmp, noopRes)
	if err != nil {
		t.Fatalf("Error creating BzFiler: %s", err)
	}

	_, err = bf.Ingest("some made up directory")
	if err == nil || !strings.Contains(err.Error(), noSuchFileOrDirMsg) {
		t.Fatalf("Expected error to contain %s, was %v", noSuchFileOrDirMsg, err)
	}
}

// Utils tests

func TestGetFileTypeDir(t *testing.T) {
	tmp, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}

	fileType, err := getFileType(tmp)
	if err != nil || fileType != fsUtilCmdDirectory {
		t.Fatalf("Expected fileType to be %s, was %s. Err: %v", fsUtilCmdDirectory, fileType, err)
	}
}

func TestGetFileTypeFile(t *testing.T) {
	tmp, err := ioutil.TempDir(tmpTest, "")
	if err != nil {
		t.Fatalf("Error creating temp dir. %v", err)
	}

	tmpFile, err := ioutil.TempFile(tmp, "")
	if err != nil {
		t.Fatalf("Error creating temp file. %v", err)
	}

	fileType, err := getFileType(tmpFile.Name())
	if err != nil || fileType != fsUtilCmdFile {
		t.Fatalf("Expected fileType to be %s, was %s. Err: %v", fsUtilCmdFile, fileType, err)
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
	id := bazel.SnapshotID(bazel.EmptySha, size)
	err := bazel.ValidateID(id)
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateIdInvalid(t *testing.T) {
	sha := "this/is/totally/wrong"
	size := int64(5)
	id := bazel.SnapshotID(sha, size)
	err := bazel.ValidateID(id)
	if err == nil {
		t.Fatalf("Expected id %s to be invalid", id)
	}
	err = bazel.ValidateID(fmt.Sprintf("bs-%s-%d", bazel.EmptySha, size))
	if err == nil {
		t.Fatalf("Expected id %s to be invalid", id)
	}
}

func TestValidateFsUtilSaveOutput(t *testing.T) {
	s := fmt.Sprintf("\n\t%s %d\n", bazel.EmptySha, int64(32))
	err := validateFsUtilSaveOutput([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateFsUtilSaveOutputInvalid(t *testing.T) {
	s := fmt.Sprintf("%s %d %s", bazel.EmptySha, int64(32), "extraVal")
	err := validateFsUtilSaveOutput([]byte(s))
	if err == nil || !strings.Contains(err.Error(), invalidSaveOutputMsg) {
		t.Fatalf("Expected error to contain \"%s\", received \"%v\"", invalidSaveOutputMsg, err)
	}
}
