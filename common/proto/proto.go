// Library for common gRPC protobuf-related tools.

package proto

import (
	"crypto/sha256"
	"fmt"

	"github.com/golang/protobuf/proto"
)

// TODO unit tests

// GetProtoSha256 returns the SHA-256 digest of the wire format of any
// protobuf message and the length in bytes of the message, or an error.
func GetSha256(pb proto.Message) (string, int64, error) {
	bytes, err := proto.Marshal(pb)
	if err != nil {
		return "", 0, fmt.Errorf("Failed to marshal protobuf message: %v", err)
	}
	sha := fmt.Sprintf("%x", sha256.Sum256(bytes))
	return sha, int64(len(bytes)), nil
}
