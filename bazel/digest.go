package bazel

// Digest utilities for Bazel

import (
	"fmt"
	"strings"

	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
)

// Validate Digest hash and size components assuming SHA256
// Size -1 indicates unknown size to support certain size-less operations
//	* this is a deviation from the public API
func IsValidDigest(hash string, size int64) bool {
	return len(hash) == 64 && size >= -1
}

// Translate a Bazel Digest into a unique resource name for use in a bundleStore
// Only use hash in store name, size is dropped
func DigestStoreName(digest *remoteexecution.Digest) string {
	if digest != nil {
		return fmt.Sprintf("%s-%s.%s", StorePrefix, digest.GetHash(), StorePrefix)
	}
	return ""
}

// Create a Digest from a proprietary string format: "<hash>/<size>".
// Returns a Digest if parsed and validated, or an error.
func DigestFromString(s string) (*remoteexecution.Digest, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Invalid format, expected '<hash>/<size>'")
	}
	snap := fmt.Sprintf("%s-%s-%s", SnapshotIDPrefix, parts[0], parts[1])
	if err := ValidateID(snap); err != nil {
		return nil, err
	}
	return DigestFromSnapshotID(snap)
}

func DigestToStr(d *remoteexecution.Digest) string {
	if d == nil || d.GetHash() == "" {
		return ""
	}
	return fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes())
}

func EmptyDigest() *remoteexecution.Digest {
	return &remoteexecution.Digest{
		Hash:      EmptySha,
		SizeBytes: EmptySize,
	}
}

func IsEmptyDigest(d *remoteexecution.Digest) bool {
	return d.GetHash() == EmptySha
}
