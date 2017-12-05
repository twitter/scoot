package cas

// Resource utils for Bazel. Resources refer to how content is read or written to the CAS

import (
	"fmt"
	"strconv"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel"
)

// Keep track of a Resource specified by a client.
// Instance - optional parameter identifying a server instance
// Digest - Bazel Digest identifier
// UUID - client identifier attached to write requests
type Resource struct {
	Instance string
	Digest   *remoteexecution.Digest
	UUID     uuid.UUID
}

func (r *Resource) String() string {
	return fmt.Sprintf("Instance: %s, Digest: %s, UUID: %s", r.Instance, r.Digest, r.UUID)
}

// Parses a name string from the Read API into a Resource for bazel artifacts.
// Valid read format: "[<instance>/]blobs/<hash>/<size>[/<filename>]"
func ParseReadResource(name string) (*Resource, error) {
	elems := strings.Split(name, "/")
	if len(elems) < 3 {
		return nil, resourceError("len elems '/' mismatch", name, ResourceReadFormatStr)
	}

	var instance, hash, sizeStr string
	if elems[0] == ResourceNameType {
		instance = bazel.DefaultInstanceName
		hash = elems[1]
		sizeStr = elems[2]
	} else if elems[1] == ResourceNameType && len(elems) > 3 {
		instance = elems[0]
		hash = elems[2]
		sizeStr = elems[3]
	} else {
		return nil, resourceError("resource type not found", name, ResourceReadFormatStr)
	}

	return ParseResource(instance, "", hash, sizeStr, name, ResourceReadFormatStr)
}

// Parses a name string from the Write API into a Resource for bazel artifacts.
// Valid read format: "[<instance>/]uploads/<uuid>/blobs/<hash>/<size>[/<filename>]"
func ParseWriteResource(name string) (*Resource, error) {
	elems := strings.Split(name, "/")
	if len(elems) < 5 {
		return nil, resourceError("len elems '/' mismatch", name, ResourceWriteFormatStr)
	}

	var id, instance, hash, sizeStr string
	var rest []string

	if elems[0] == ResourceNameAction {
		instance = bazel.DefaultInstanceName
		rest = elems[1:]
	} else if elems[1] == ResourceNameAction && len(elems) > 4 {
		instance = elems[0]
		rest = elems[2:]
	} else {
		return nil, resourceError("resource action not found", name, ResourceWriteFormatStr)
	}

	if rest[1] != ResourceNameType {
		return nil, resourceError("resource type not found", name, ResourceWriteFormatStr)
	}

	id = rest[0]
	hash = rest[2]
	sizeStr = rest[3]

	return ParseResource(instance, id, hash, sizeStr, name, ResourceWriteFormatStr)
}

// Underlying Resource parser from separated URI components
func ParseResource(instance, id, hash, sizeStr, name, format string) (*Resource, error) {
	var uid uuid.UUID
	if id != "" {
		u, err := uuid.ParseHex(id)
		if err != nil {
			return nil, resourceError("uuid invalid", name, format)
		}
		uid = *u
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return nil, resourceError("size value could not be parsed as int64", name, format)
	}

	if !bazel.IsValidDigest(hash, size) {
		return nil, resourceError("digest hash/size invalid", name, format)
	}

	return &Resource{Instance: instance, Digest: &remoteexecution.Digest{Hash: hash, SizeBytes: size}, UUID: uid}, nil
}

// helper for descriptive resource error messages
func resourceError(reason, name, format string) error {
	return fmt.Errorf("Invalid resource name format (%s) from: %q, expected: %q", reason, name, format)
}
