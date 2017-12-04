package bazel

// Digest utilities for Bazel

import (
	"fmt"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
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
