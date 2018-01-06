package proto

import (
	"testing"

	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/empty"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

func TestGetEmptySha256(t *testing.T) {
	e := empty.Empty{}
	s, l, err := GetSha256(&e)
	if err != nil {
		t.Fatalf("GetSha256 failure: %v", err)
	}
	// This is what sha-256'ing no data returns
	// This is as good as precomputing any old data structure, presumably
	nilDataSha := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if s != nilDataSha {
		t.Fatalf("Expected known sha for nil/empty data %s, got: %s", nilDataSha, s)
	}
	if l != 0 {
		t.Fatalf("Expected zero length data, got: %d", l)
	}
}

// This is a canary test of sorts for generating digests of Action data from bazel
// ExecuteRequests. If this starts to fail, it indicates an instability in hashing Action messages.
func TestGetActionSha256(t *testing.T) {
	a := &remoteexecution.Action{
		CommandDigest:     &remoteexecution.Digest{Hash: "abc123", SizeBytes: 10},
		InputRootDigest:   &remoteexecution.Digest{Hash: "def456", SizeBytes: 20},
		OutputFiles:       []string{"output"},
		OutputDirectories: []string{"/data"},
		Platform: &remoteexecution.Platform{
			Properties: []*remoteexecution.Platform_Property{&remoteexecution.Platform_Property{Name: "abc", Value: "123"}},
		},
		Timeout:    GetDurationFromMs(60000),
		DoNotCache: true,
	}
	s, _, err := GetSha256(a)
	if err != nil {
		t.Fatalf("GetSha256 failure: %v", err)
	}
	expectedSha := "4d1e3f9c9ff80c29be01efb35ddeb7c11cb3f91c79ee183aaaf52c3623c8772c"
	if s != expectedSha {
		t.Fatalf("Expected known sha for message data: %s, got: %s", expectedSha, s)
	}
}

func TestMsDuration(t *testing.T) {
	d := duration.Duration{Seconds: 3, Nanos: 5000004}
	ms := GetMsFromDuration(&d)
	if ms != 3005 {
		t.Fatalf("Expected 3005, got: %dms", ms)
	}

	dp := GetDurationFromMs(ms)
	if dp == nil {
		t.Fatalf("Unexpected nil result from GetDurationFromMs(%d)", ms)
	}
	if dp.GetSeconds() != 3 || dp.GetNanos() != 5000000 {
		t.Fatalf("Expected 3s 5000000ns, got: %ds %dns", dp.GetSeconds(), dp.GetNanos())
	}
}
