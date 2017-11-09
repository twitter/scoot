package execution

import (
	"testing"

	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	scootproto "github.com/twitter/scoot/common/proto"
)

// Determine that Execute can accept a well-formed request and returns a well-formed response
func TestExecuteStub(t *testing.T) {
	s := executionServer{}
	ctx := context.Background()

	cmd := remoteexecution.Command{Arguments: []string{"/bin/true"}}
	cmdSha, cmdLen, err := scootproto.GetSha256(&cmd)
	if err != nil {
		t.Fatalf("Failed to get sha: %v", err)
	}
	dir := remoteexecution.Directory{}
	dirSha, dirLen, err := scootproto.GetSha256(&dir)
	if err != nil {
		t.Fatalf("Failed to get sha: %v", err)
	}

	a := remoteexecution.Action{
		CommandDigest:   &remoteexecution.Digest{Hash: cmdSha, SizeBytes: cmdLen},
		InputRootDigest: &remoteexecution.Digest{Hash: dirSha, SizeBytes: dirLen},
	}
	req := remoteexecution.ExecuteRequest{
		Action:              &a,
		InstanceName:        "test",
		SkipCacheLookup:     true,
		TotalInputFileCount: 0,
		TotalInputFileBytes: 0,
	}

	res, err := s.Execute(ctx, &req)
	if err != nil {
		t.Fatalf("Non-nil error from Execute: %v", err)
	}

	done := res.GetDone()
	if !done {
		t.Fatal("Expected response to be done")
	}
	metadataAny := res.GetMetadata()
	if metadataAny == nil {
		t.Fatalf("Nil metadata from operation: %s", res)
	}
	execResAny := res.GetResponse()
	if execResAny == nil {
		t.Fatalf("Nil response from operation: %s", res)
	}

	metadata := remoteexecution.ExecuteOperationMetadata{}
	execRes := remoteexecution.ExecuteResponse{}
	err = ptypes.UnmarshalAny(metadataAny, &metadata)
	if err != nil {
		t.Fatalf("Failed to unmarshal metadata from any: %v", err)
	}
	err = ptypes.UnmarshalAny(execResAny, &execRes)
	if err != nil {
		t.Errorf("Failed to unmarshal response from any: %v", err)
	}
}
