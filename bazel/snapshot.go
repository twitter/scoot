package bazel

// Snapshot-related utilities for Bazel

import (
	"fmt"
	"strconv"
	"strings"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// generate - from Digest??
func getSha(id string) (string, error) {
	s, err := splitId(id)
	if err != nil {
		return "", err
	}
	return s[1], nil
}

func getSize(id string) (int64, error) {
	s, err := splitId(id)
	if err != nil {
		return 0, err
	}
	size, err := strconv.ParseInt(s[2], 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func splitId(id string) ([]string, error) {
	s := strings.Split(id, "-")
	if len(s) < 3 || s[0] != SnapshotIDPrefix {
		return nil, fmt.Errorf("%s %s", InvalidIDMsg, id)
	}
	return s, nil
}

func SnapshotID(sha string, size int64) string {
	return fmt.Sprintf("%s-%s-%d", SnapshotIDPrefix, sha, size)
}

func DigestSnapshotID(d *remoteexecution.Digest) string {
	if d == nil {
		return ""
	}
	return SnapshotID(d.GetHash(), d.GetSizeBytes())
}
