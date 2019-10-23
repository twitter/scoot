package execution

import (
	"reflect"
	"testing"

	"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/genproto/googleapis/rpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/remoteexecution"
)

func TestExtractOpFromJsonError(t *testing.T) {
	opBytes := []byte(`{"name":"testName","metadata":{"type_url":"type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata","value":"testVal"},"done":true,"Result":{"Error":{"code":1,"message":"CANCELLED"}}}`)

	metadata := &any.Any{
		TypeUrl: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata",
		Value:   []byte("testVal"),
	}
	opErr := &longrunning.Operation_Error{
		Error: &status.Status{
			Code:    int32(1),
			Message: "CANCELLED",
		},
	}
	expOp := &longrunning.Operation{
		Name:     "testName",
		Metadata: metadata,
		Done:     true,
		Result:   opErr,
	}

	gotOp, err := ExtractOpFromJson(opBytes)
	if err != nil {
		t.Fatalf("Received error extracting operation from json: %s", err)
	}
	if !reflect.DeepEqual(gotOp, expOp) {
		t.Fatalf("Expected gotOp to equal expOp.\ngotOp: %+v\nexpOp: %+v", gotOp, expOp)
	}
}

func TestExtractOpFromJsonResponse(t *testing.T) {
	opBytes := []byte(`{"name":"testName","metadata":{"type_url":"type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata","value":"testVal"},"done":true,"Result":{"Response":{"type_url":"type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata","value":"testVal"}}}`)

	metadata := &any.Any{
		TypeUrl: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata",
		Value:   []byte("testVal"),
	}
	opResp := &longrunning.Operation_Response{
		Response: &any.Any{
			TypeUrl: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata",
			Value:   []byte("testVal"),
		},
	}
	expOp := &longrunning.Operation{
		Name:     "testName",
		Metadata: metadata,
		Done:     true,
		Result:   opResp,
	}

	gotOp, err := ExtractOpFromJson(opBytes)
	if err != nil {
		t.Fatalf("Received error extracting operation from json: %s", err)
	}
	if !reflect.DeepEqual(gotOp, expOp) {
		t.Fatalf("Expected gotOp to equal expOp.\ngotOp: %+v\nexpOp: %+v", gotOp, expOp)
	}
}

func TestOperationToJson(t *testing.T) {
	eom := &remoteexecution.ExecuteOperationMetadata{
		Stage:        remoteexecution.ExecuteOperationMetadata_COMPLETED,
		ActionDigest: &remoteexecution.Digest{Hash: bazel.EmptySha, SizeBytes: bazel.EmptySize},
	}
	eomAsPBAny, err := marshalAny(eom)
	if err != nil {
		t.Fatalf("Failed to marshal: %s", err)
	}

	er := &remoteexecution.ExecuteResponse{
		Result:       &remoteexecution.ActionResult{},
		CachedResult: false,
		Status:       &status.Status{},
	}
	resAsPBAny, err := marshalAny(er)
	if err != nil {
		t.Fatalf("Failed to marshal: %s", err)
	}

	op := &longrunning.Operation{
		Name:     "test",
		Metadata: eomAsPBAny,
		Done:     true,
		Result:   &longrunning.Operation_Response{Response: resAsPBAny},
	}

	_, err = OperationToJson(op)
	if err != nil {
		t.Fatalf("Failed to parse to json: %s", err)
	}
}
