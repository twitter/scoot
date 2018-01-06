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
func DigestSnapshotID(d *remoteexecution.Digest) string {
	if d == nil {
		return ""
	}
	return SnapshotID(d.GetHash(), d.GetSizeBytes())
}

// Checks that ID is well formed
func ValidateID(id string) error {
	sha, err := GetSha(id)
	if err != nil {
		return err
	}
	size, err := GetSize(id)
	if err != nil {
		return err
	}
	if !IsValidDigest(sha, size) {
		return fmt.Errorf("Error: Invalid digest. SHA: %s, size: %d", sha, size)
	}
	return nil
}

// Get sha portion from valid bazel SnapshotID
func GetSha(id string) (string, error) {
	s, err := SplitID(id)
	if err != nil {
		return "", err
	}
	return s[1], nil
}

// Get size portion from valid bazel SnapshotID
func GetSize(id string) (int64, error) {
	s, err := SplitID(id)
	if err != nil {
		return 0, err
	}
	size, err := strconv.ParseInt(s[2], 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Split valid bazel SnapshotID into prefix, sha and size components as strings
func SplitID(id string) ([]string, error) {
	s := strings.Split(id, "-")
	if len(s) < 3 || s[0] != SnapshotIDPrefix {
		return nil, fmt.Errorf("%s %s", InvalidIDMsg, id)
	}
	return s, nil
}
