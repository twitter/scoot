package bazel

import (
	"fmt"
	"strconv"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

// TODO belongs here or with CAS? - will Execute need? "store.go" name - maybe resource.go is better?
// maybe Digests in top level. Resources are in CAS (Actions used in Exec also refer to Digests)

// TODO tests

type Resource struct {
	Instance string
	Digest   *remoteexecution.Digest
	WriterID uuid.UUID
}

// Parses a name into a Resource identifier for bazel artifacts.
// Valid format: "[<instance>/]blobs/<hash>/<size>[/<filename>]"
// Instance and filename are optional and ignored
// TODO update comment. error thing got carried away
func ParseReadResource(name string) (*Resource, error) {
	elems := strings.Split(name, "/")
	if len(elems) < 3 {
		return nil, ResourceError("len elems '/' mismatch", name, ResourceReadFormatStr)
	}

	var instance, hash, sizeStr string
	if elems[0] == ResourceTypeStr {
		instance = DefaultInstanceName
		hash = elems[1]
		sizeStr = elems[2]
	} else if elems[1] == ResourceTypeStr && len(elems) > 3 {
		instance = elems[0]
		hash = elems[2]
		sizeStr = elems[3]
	} else {
		return nil, ResourceError("resource type not found", name, ResourceReadFormatStr)
	}

	return ParseResource(name, instance, "", hash, sizeStr, ResourceReadFormatStr)
}

func ParseWriteResource(name string) (*Resource, error) {
	elems := strings.Split(name, "/")
	if len(elems) < 5 {
		return nil, ResourceError("len elems '/' mismatch", name, ResourceWriteFormatStr)
	}

	var id, instance, hash, sizeStr string
	var rest []string

	if elems[0] == ResourceActionStr {
		instance = DefaultInstanceName
		rest = elems[1:]
	} else if elems[1] == ResourceActionStr && len(elems) > 4 {
		instance = elems[0]
		rest = elems[2:]
	} else {
		return nil, ResourceError("resource action not found", name, ResourceWriteFormatStr)
	}

	if rest[1] != ResourceTypeStr {
		return nil, ResourceError("resource type not found", name, ResourceWriteFormatStr)
	}

	id = rest[0]
	hash = rest[2]
	sizeStr = rest[3]

	return ParseResource(name, instance, id, hash, sizeStr, ResourceWriteFormatStr)
}

func ParseResource(name, instance, id, hash, sizeStr, format string) (*Resource, error) {
	var uid uuid.UUID
	if id != "" {
		u, err := uuid.ParseHex(id)
		if err != nil {
			return nil, ResourceError("uuid invalid", name, format)
		}
		uid = *u
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return nil, ResourceError("size value could not be parsed as int64", name, format)
	}

	if !IsValidDigest(hash, size) {
		return nil, ResourceError("digest hash/size invalid", name, format)
	}

	return &Resource{Instance: instance, Digest: &remoteexecution.Digest{Hash: hash, SizeBytes: size}, WriterID: uid}, nil
}

// Validate Digest hash and size components assuming SHA256
func IsValidDigest(hash string, size int64) bool {
	return len(hash) == 64 && size >= 0
}

func ResourceError(reason, name, format string) error {
	return fmt.Errorf("Invalid resource name format (%s) from: %q, expected: %q", reason, name, format)
}

// Translate a Bazel Digest into a unique resource name for use in a bundleStore
func DigestStoreName(digest *remoteexecution.Digest) string {
	if digest != nil {
		return fmt.Sprintf("%s-%s-%d.%s", StorePrePostFix, digest.GetHash(), digest.GetSizeBytes(), StorePrePostFix)
	}
	return ""
}
