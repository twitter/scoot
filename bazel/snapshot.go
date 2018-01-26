package bazel

// Snapshot-related utilities for Bazel

import (
	"fmt"
	"strconv"
	"strings"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// Generate a SnapshotID based on digest sha and size
func SnapshotID(sha string, size int64) string {
	return fmt.Sprintf("%s-%s-%d", SnapshotIDPrefix, sha, size)
}

// Generate a SnapshotID directly from a Digest
func SnapshotIDFromDigest(d *remoteexecution.Digest) string {
	if d == nil {
		return ""
	}
	return SnapshotID(d.GetHash(), d.GetSizeBytes())
}

// Checks that ID is well formed
func ValidateID(id string) error {
	sha, size, err := GetShaAndSize(id)
	if err != nil {
		return err
	}
	if !IsValidDigest(sha, size) {
		return fmt.Errorf("Error: Invalid digest. SHA: %s, size: %d", sha, size)
	}
	return nil
}

// Get sha and size components from valid bazel SnapshotID
func GetShaAndSize(id string) (string, int64, error) {
	s, err := splitID(id)
	if err != nil {
		return "", 0, err
	}
	size, err := strconv.ParseInt(s[2], 10, 64)
	if err != nil {
		return "", 0, err
	}
	return s[1], size, nil
}

// Get a remoteexecution Digest from SnapshotID
func DigestFromSnapshotID(id string) (*remoteexecution.Digest, error) {
	sha, size, err := GetShaAndSize(id)
	if err != nil {
		return nil, err
	}
	return &remoteexecution.Digest{Hash: sha, SizeBytes: size}, nil
}

// Split valid bazel SnapshotID into prefix, sha and size components as strings
func splitID(id string) ([]string, error) {
	s := strings.Split(id, "-")
	if len(s) < 3 || s[0] != SnapshotIDPrefix {
		return nil, fmt.Errorf("%s %s", InvalidIDMsg, id)
	}
	return s, nil
}
