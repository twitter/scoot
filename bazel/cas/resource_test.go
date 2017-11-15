package cas

import (
	"testing"

	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

func TestResourceComponents(t *testing.T) {
	id := "00000000-0000-0000-0000-000000000000"
	hash := "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	sizeStr := "10"
	if _, err := ParseResource("", id, hash, sizeStr, "", ""); err == nil {
		t.Fatalf("Expected failure to parse invalid uuid str: %s", id)
	}

	id = "6ba7b814-9dad-11d1-80b4-00c04fd430c8"
	hash = "abc123"
	if _, err := ParseResource("", id, hash, sizeStr, "", ""); err == nil {
		t.Fatalf("Expected failure to parse invalid hash str: %s", hash)
	}

	hash = "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b"
	sizeStr = "-1"
	if _, err := ParseResource("", id, hash, sizeStr, "", ""); err == nil {
		t.Fatalf("Expected failure to parse invalid size: %s", sizeStr)
	}

	sizeStr = "10"
	if _, err := ParseResource("", id, hash, sizeStr, "", ""); err != nil {
		t.Fatalf("Unexpected failure parsing id/hash/size: %s/%s/%s", id, hash, sizeStr)
	}
}

func TestParseReadResource(t *testing.T) {
	name := "just clearly wrong"
	if r, err := ParseReadResource(name); err == nil {
		t.Fatalf("Expected failure to parse resource name without slashes: %s, got resource: %s", name, r)
	}

	name = "abc123/1/foo.f"
	if r, err := ParseReadResource(name); err == nil {
		t.Fatalf("Expected failure to parse resource name without type str: %s, got resource: %s", name, r)
	}

	name = "blobs/01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b/5"
	expected := &Resource{
		Digest: &remoteexecution.Digest{
			Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
			SizeBytes: 5,
		},
	}
	r, err := ParseReadResource(name)
	if err != nil {
		t.Fatalf("Failed to parse valid resource name: %s: %v", name, err)
	}
	if !resourceEq(r, expected) {
		t.Fatalf("Parsed resource not as expected: %s, got: %s", expected, r)
	}

	name = "instance/blobs/01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b/5/foo/bar.f"
	expected = &Resource{
		Instance: "instance",
		Digest: &remoteexecution.Digest{
			Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
			SizeBytes: 5,
		},
	}
	r, err = ParseReadResource(name)
	if err != nil {
		t.Fatalf("Failed to parse valid resource name: %s: %v", name, err)
	}
	if !resourceEq(r, expected) {
		t.Fatalf("Parsed resource not as expected: %s, got: %s", expected, r)
	}
}

func TestParseWriteResource(t *testing.T) {
	name := "just clearly wrong"
	if r, err := ParseWriteResource(name); err == nil {
		t.Fatalf("Expected failure to parse resource name without slashes: %s, got resource: %s", name, r)
	}

	name = "abc123/1/foo.f"
	if r, err := ParseWriteResource(name); err == nil {
		t.Fatalf("Expected failure to parse resource name without type/action str: %s, got resource: %s", name, r)
	}

	name = "uploads/6ba7b814-9dad-11d1-80b4-00c04fd430c8/blobs/01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b/5"
	uid, err := uuid.ParseHex("6ba7b814-9dad-11d1-80b4-00c04fd430c8")
	if err != nil {
		t.Fatalf("Failed to parse valid UUID: %s: %v", "6ba7b814-9dad-11d1-80b4-00c04fd430c8", err)
	}
	expected := &Resource{
		Digest: &remoteexecution.Digest{
			Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
			SizeBytes: 5,
		},
		UUID: *uid,
	}
	r, err := ParseWriteResource(name)
	if err != nil {
		t.Fatalf("Failed to parse valid resource name: %s: %v", name, err)
	}
	if !resourceEq(r, expected) {
		t.Fatalf("Parsed resource not as expected: %s, got: %s", expected, r)
	}

	name = "instance/uploads/6ba7b814-9dad-11d1-80b4-00c04fd430c8/blobs/01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b/5/foo/bar.f"
	expected = &Resource{
		Instance: "instance",
		Digest: &remoteexecution.Digest{
			Hash:      "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b",
			SizeBytes: 5,
		},
		UUID: *uid,
	}
	r, err = ParseWriteResource(name)
	if err != nil {
		t.Fatalf("Failed to parse valid resource name: %s: %v", name, err)
	}
	if !resourceEq(r, expected) {
		t.Fatalf("Parsed resource not as expected: %s, got: %s", expected, r)
	}
}

func resourceEq(r1 *Resource, r2 *Resource) bool {
	if (r1 == nil) != (r2 == nil) {
		return false
	}
	if r1.Instance != r2.Instance {
		return false
	}
	if r1.UUID != r2.UUID {
		return false
	}
	if r1.Digest.GetHash() != r2.Digest.GetHash() || r1.Digest.GetSizeBytes() != r2.Digest.GetSizeBytes() {
		return false
	}
	return true
}
