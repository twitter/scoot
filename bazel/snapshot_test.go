package bazel

import (
	"fmt"
	"strings"
	"testing"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

var EmptySha string = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
var size5 int64 = 5

func TestGetShaAndSize(t *testing.T) {
	id := SnapshotID(EmptySha, size5)
	resultSha, resultSize, err := GetShaAndSize(id)
	if err != nil {
		t.Fatal(err)
	}
	if resultSha != EmptySha {
		t.Fatalf("Expected %s, got %s", EmptySha, resultSha)
	}
	if resultSize != size5 {
		t.Fatalf("Expected %d, got %d", size5, resultSize)
	}
}

func TestSplitIdValid(t *testing.T) {
	id := SnapshotID(EmptySha, size5)
	result, err := splitID(id)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{SnapshotIDPrefix, EmptySha, "5"}
	for idx, _ := range result {
		if result[idx] != expected[idx] {
			t.Fatalf("Expected %v, received %v", expected, result)
		}
	}
}

func TestSplitIdInvalid(t *testing.T) {
	id := fmt.Sprintf("bs-%s-%d", EmptySha, size5)
	_, err := splitID(id)
	if err == nil || !strings.Contains(err.Error(), InvalidIDMsg) {
		t.Fatalf("Expected error to contain \"%s\", received \"%v\"", InvalidIDMsg, err)
	}
}

func TestSnapshotIDFromDigest(t *testing.T) {
	d := &remoteexecution.Digest{Hash: EmptySha, SizeBytes: size5}
	id := SnapshotIDFromDigest(d)
	if err := ValidateID(id); err != nil {
		t.Fatalf("Unexpected invalid snapshot ID: %s", err)
	}
}

func TestGetDigestFromSnapshotID(t *testing.T) {
	id := SnapshotID(EmptySha, size5)
	d, err := DigestFromSnapshotID(id)
	if err != nil {
		t.Fatalf("Error creating digest from SnapshotID: %s", err)
	}
	if d.GetHash() != EmptySha {
		t.Fatalf("Expected %s, got %s", EmptySha, d.GetHash())
	}
	if d.GetSizeBytes() != size5 {
		t.Fatalf("Expected %d, got %d", size5, d.GetSizeBytes())
	}
}
