package bazel

import (
	"fmt"
	"testing"

	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
)

func TestValidDigest(t *testing.T) {
	hash := "abc123"
	if IsValidDigest(hash, 0) {
		t.Fatalf("hash/size %s/0 should be invalid", hash)
	}

	hash = "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	if !IsValidDigest(hash, 0) {
		t.Fatalf("hash/size %s/0 should be valid", hash)
	}

	if IsValidDigest(hash, -2) {
		t.Fatalf("hash/size %s/-2 should be invalid", hash)
	}
}

func TestDigestStoreName(t *testing.T) {
	var d *remoteexecution.Digest = nil

	if n := DigestStoreName(d); n != "" {
		t.Fatalf("Expected empty store name from nil digest, got: %s", n)
	}

	hash := "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	var size int64 = 123
	d = &remoteexecution.Digest{Hash: hash, SizeBytes: size}

	expected := fmt.Sprintf("%s-%s.%s", StorePrefix, hash, StorePrefix)
	if n := DigestStoreName(d); n != expected {
		t.Fatalf("Wrong digest store name, expected: %s, got: %s", expected, n)
	}
}

func TestDigestFromString(t *testing.T) {
	s := "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b/123"
	_, err := DigestFromString(s)
	if err != nil {
		t.Fatalf("Failed to create digest from string: %s", err)
	}
}

func TestEmptyDigest(t *testing.T) {
	d1 := &remoteexecution.Digest{
		Hash:      EmptySha,
		SizeBytes: EmptySize,
	}
	d2 := &remoteexecution.Digest{
		Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
		SizeBytes: 123,
	}
	if !IsEmptyDigest(d1) {
		t.Fatal("Empty hash/size not recognized")
	}
	if IsEmptyDigest(d2) {
		t.Fatal("Non-empty hash/size false positive")
	}
}

func TestDigestsEqual(t *testing.T) {
	var d0 *remoteexecution.Digest
	d1 := &remoteexecution.Digest{
		Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
		SizeBytes: 123,
	}
	d2 := &remoteexecution.Digest{
		Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
		SizeBytes: 123,
	}

	if DigestsEqual(d0, d1) {
		t.Fatalf("Digests not equal, wanted equal (%s, %s)", d0, d1)
	}
	if !DigestsEqual(d1, d2) {
		t.Fatalf("Digests equal, wanted not equal (%s, %s)", d1, d2)
	}
}
