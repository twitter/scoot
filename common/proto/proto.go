// Library for common gRPC protobuf-related tools.
package proto

import (
	"crypto/sha256"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/duration"
)

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

func GetMsFromDuration(d *duration.Duration) int64 {
	if d == nil {
		return 0
	}
	return d.GetSeconds()*1000 + int64(d.GetNanos()/1000000)
}

func GetDurationFromMs(ms int64) duration.Duration {
	return duration.Duration{Seconds: int64(ms / 1000), Nanos: int32((ms % 1000) * 1000000)}
}
