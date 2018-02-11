package bazelapi

import (
	"testing"

	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	scootproto "github.com/twitter/scoot/common/proto"
)

func TestDomainThriftDomainExecReq(t *testing.T) {
	er := &ExecuteRequest{
		Request: &remoteexecution.ExecuteRequest{
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

	tr := MakeExecReqThriftFromDomain(er)
	if tr == nil {
		t.Fatal("Unexpected nil thrift result")
	}

	result := MakeExecReqDomainFromThrift(tr)
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
		result.Request.Action.DoNotCache != er.Request.Action.DoNotCache ||
		result.String() != er.String() {
		t.Fatalf("Unexpected output from result\ngot:      %v\nexpected: %v", result, er)
	}
}

func TestDomainThriftDomainActionRes(t *testing.T) {
	ar := &ActionResult{
		Result: &remoteexecution.ActionResult{
			StdoutDigest: &remoteexecution.Digest{Hash: "curry", SizeBytes: 30},
			StderrDigest: &remoteexecution.Digest{Hash: "carr", SizeBytes: 4},
			StdoutRaw:    []byte("durant"),
			StderrRaw:    []byte("lynch"),
			OutputFiles: []*remoteexecution.OutputFile{
				&remoteexecution.OutputFile{
					Digest:       &remoteexecution.Digest{Hash: "green", SizeBytes: 23},
					Path:         "crabtree",
					Content:      []byte("thompson"),
					IsExecutable: true,
				},
			},
			OutputDirectories: []*remoteexecution.OutputDirectory{
				&remoteexecution.OutputDirectory{
					TreeDigest: &remoteexecution.Digest{Hash: "cooper", SizeBytes: 89},
					Path:       "iguodala",
				},
			},
			ExitCode: 1,
		},
	}

	tr := MakeActionResultThriftFromDomain(ar)
	if tr == nil {
		t.Fatal("Unexpected nil thrift result")
	}

	result := MakeActionResultDomainFromThrift(tr)
	if result == nil {
		t.Fatal("Unexpected nil domain result")
	}

	if result.Result.StdoutDigest.Hash != ar.Result.StdoutDigest.Hash ||
		result.Result.StdoutDigest.SizeBytes != ar.Result.StdoutDigest.SizeBytes ||
		result.Result.StderrDigest.Hash != ar.Result.StderrDigest.Hash ||
		result.Result.StderrDigest.SizeBytes != ar.Result.StderrDigest.SizeBytes ||
		string(result.Result.StdoutRaw) != string(ar.Result.StdoutRaw) ||
		string(result.Result.StderrRaw) != string(ar.Result.StderrRaw) ||
		result.Result.OutputFiles[0].Digest.Hash != ar.Result.OutputFiles[0].Digest.Hash ||
		result.Result.OutputFiles[0].Digest.SizeBytes != ar.Result.OutputFiles[0].Digest.SizeBytes ||
		result.Result.OutputFiles[0].Path != ar.Result.OutputFiles[0].Path ||
		string(result.Result.OutputFiles[0].Content) != string(ar.Result.OutputFiles[0].Content) ||
		result.Result.OutputFiles[0].IsExecutable != ar.Result.OutputFiles[0].IsExecutable ||
		result.Result.OutputDirectories[0].TreeDigest.Hash != ar.Result.OutputDirectories[0].TreeDigest.Hash ||
		result.Result.OutputDirectories[0].TreeDigest.SizeBytes != ar.Result.OutputDirectories[0].TreeDigest.SizeBytes ||
		result.Result.ExitCode != ar.Result.ExitCode ||
		result.String() != ar.String() {
		t.Fatalf("Unexpected output from result\ngot:      %v\nexpected: %v", result, ar)
	}
}
