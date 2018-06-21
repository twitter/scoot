package cas

// Resource utils for Bazel. Resources refer to how content is read or written to the CAS

import (
	"fmt"
	"strconv"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"

	"github.com/twitter/scoot/bazel"
)

// Keep track of a Resource specified by a client.
// Instance - optional parameter identifying a server instance
// Digest - Bazel Digest identifier
// UUID - client identifier attached to write requests
//	Unused by Scoot currently except for tracking/logging
type Resource struct {
	Instance string
	Digest   *remoteexecution.Digest
	UUID     uuid.UUID
}

func (r *Resource) String() string {
	return fmt.Sprintf("Instance: %s, Digest: %s, UUID: %s", r.Instance, r.Digest, r.UUID)
}

// Return a valid read resource string based on individual components. Errors on invalid inputs.
func GetReadResourceName(instance, hash string, size int64, fname string) (string, error) {
	rname := ""
	if instance != "" {
		rname += fmt.Sprintf("%s/", instance)
	}
	rname += fmt.Sprintf("%s/%s/%d", ResourceNameType, hash, size)
	if fname != "" {
		rname += fmt.Sprintf("/%s", fname)
	}
	if _, err := ParseReadResource(rname); err != nil {
		return "", err
	}
	return rname, nil
}

func GetDefaultReadResourceName(hash string, size int64) (string, error) {
	return GetReadResourceName("", hash, size, "")
}

// Parses a name string from the Read API into a Resource for bazel artifacts.
// Valid read format: "[<instance>/]blobs/<hash>/<size>[/<filename>]"
// Scoot does not currently use/track the filename portion of resource names
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

// Return a valid write resource string based on individual components. Errors on invalid inputs
func GetWriteResourceName(instance, uuid, hash string, size int64, fname string) (string, error) {
	wname := ""
	if instance != "" {
		wname += fmt.Sprintf("%s/", instance)
	}
	wname += fmt.Sprintf("%s/%s/%s/%s/%d", ResourceNameAction, uuid, ResourceNameType, hash, size)
	if fname != "" {
		wname += fmt.Sprintf("/%s", fname)
	}
	if _, err := ParseWriteResource(wname); err != nil {
		return "", err
	}
	return wname, nil
}

func GetDefaultWriteResourceName(uuid, hash string, size int64) (string, error) {
	return GetWriteResourceName("", uuid, hash, size, "")
}

// Parses a name string from the Write API into a Resource for bazel artifacts.
// Valid read format: "[<instance>/]uploads/<uuid>/blobs/<hash>/<size>[/<filename>]"
// Scoot does not currently use/track the filename portion of resource names
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
