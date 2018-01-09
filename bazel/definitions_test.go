package bazel

import (
	"testing"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	scootproto "github.com/twitter/scoot/common/proto"
)

func TestDomainThriftDomain(t *testing.T) {
	er := &ExecuteRequest{
		Request: remoteexecution.ExecuteRequest{
			InstanceName:    "test",
			SkipCacheLookup: true,
			Action: &remoteexecution.Action{
				CommandDigest:     &remoteexecution.Digest{Hash: "abc123", SizeBytes: 10},
				InputRootDigest:   &remoteexecution.Digest{Hash: "def456", SizeBytes: 20},
				OutputFiles:       []string{"output"},
				OutputDirectories: []string{"/data"},
				Platform: &remoteexecution.Platform{
					Properties: []*remoteexecution.Platform_Property{
						&remoteexecution.Platform_Property{Name: "propname", Value: "propvalue"},
					},
				},
				Timeout:    scootproto.GetDurationFromMs(60000),
				DoNotCache: true,
			},
		},
	}

	tr := MakeThriftFromDomain(er)
	if tr == nil {
		t.Fatal("Unexpected nil thrift result")
	}

	result := MakeDomainFromThrift(tr)
	if result == nil {
		t.Fatal("Unexpected nil domain result")
	}

	if result.Request.InstanceName != er.Request.InstanceName ||
		result.Request.SkipCacheLookup != er.Request.SkipCacheLookup ||
		result.Request.Action.CommandDigest.Hash != er.Request.Action.CommandDigest.Hash ||
		result.Request.Action.InputRootDigest.SizeBytes != er.Request.Action.InputRootDigest.SizeBytes ||
		result.Request.Action.OutputFiles[0] != er.Request.Action.OutputFiles[0] ||
		result.Request.Action.OutputDirectories[0] != er.Request.Action.OutputDirectories[0] ||
		result.Request.Action.Platform.Properties[0].Value != er.Request.Action.Platform.Properties[0].Value ||
		scootproto.GetMsFromDuration(result.Request.Action.Timeout) != scootproto.GetMsFromDuration(er.Request.Action.Timeout) ||
		result.Request.Action.DoNotCache != er.Request.Action.DoNotCache {
		t.Fatalf("Unexpected output from result\ngot:      %v\nexpected: %v", result, er)
	}
}
