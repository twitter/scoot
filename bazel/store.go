package bazel

import (
	"fmt"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// Translate a Bazel Digest into a unique resource name for use in a bundleStore
func DigestStoreName(digest *remoteexecution.Digest) string {
	return fmt.Sprintf("%s-%s-%d.%s", StorePrePostFix, digest.GetHash(), digest.GetSizeBytes(), StorePrePostFix)
}
