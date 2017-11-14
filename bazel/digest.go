package bazel

// Digest utilities for Bazel

import (
	"fmt"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// Validate Digest hash and size components assuming SHA256
func IsValidDigest(hash string, size int64) bool {
	return len(hash) == 64 && size >= 0
}

// Translate a Bazel Digest into a unique resource name for use in a bundleStore
func DigestStoreName(digest *remoteexecution.Digest) string {
	if digest != nil {
		return fmt.Sprintf("%s-%s-%d.%s", StorePrePostFix, digest.GetHash(), digest.GetSizeBytes(), StorePrePostFix)
	}
	return ""
}
