package bazel

import (
	"fmt"
	"strconv"
	"strings"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// TODO belongs here or with CAS? - will Execute need? "store" name?

// TODO tests

type Resource struct {
	Instance string
	Digest   *remoteexecution.Digest
}

// Parses a name into a Resource identifier for bazel artifacts.
// Valid format: "<instance>/blobs/<hash>/<size>[/<filename>]"
// Instance and filename are ignored, filename is optional
func ParseResource(name string) (*Resource, error) {
	elems := strings.Split(name, "/")
	if len(elems) < 3 {
		return nil, ResourceError("len elems '/' mismatch", name)
	}

	var hash, instance string
	var size int64
	var err error

	if elems[0] == ResourceTypeStr {
		instance = DefaultInstanceName
		hash = elems[1]
		size, err = strconv.ParseInt(elems[2], 10, 64)
	} else if elems[1] == ResourceTypeStr && len(elems) > 3 {
		instance = elems[0]
		hash = elems[2]
		size, err = strconv.ParseInt(elems[3], 10, 64)
	} else {
		return nil, ResourceError("resource type not found", name)
	}

	if err != nil {
		return nil, ResourceError("size value could not be parsed as int64", name)
	}
	if !IsValidDigest(hash, size) {
		return nil, ResourceError("digest hash/size invalid", name)
	}

	return &Resource{Instance: instance, Digest: &remoteexecution.Digest{Hash: hash, SizeBytes: size}}, nil
}

// Validate Digest hash and size components assuming SHA256
func IsValidDigest(hash string, size int64) bool {
	return len(hash) == 64 && size >= 0
}

func ResourceError(reason, name string) error {
	return fmt.Errorf("Invalid resource name format (%s) from: %q, expected: %q", reason, name, ResourceFormatStr)
}

// Translate a Bazel Digest into a unique resource name for use in a bundleStore
func DigestStoreName(digest *remoteexecution.Digest) string {
	if digest != nil {
		return fmt.Sprintf("%s-%s-%d.%s", StorePrePostFix, digest.GetHash(), digest.GetSizeBytes(), StorePrePostFix)
	}
	return ""
}
