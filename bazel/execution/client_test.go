package execution

//go:generate mockgen -destination=mock_remoteexecution/execclient_mock.go github.com/twitter/scoot/bazel/remoteexecution ExecutionClient,ActionCacheClient,ContentAddressableStorageClient
//go:generate mockgen -destination=mock_longrunning/longrunning_mock.go google.golang.org/genproto/googleapis/longrunning OperationsClient

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/bazel/execution/mock_longrunning"
	"github.com/twitter/scoot/bazel/execution/mock_remoteexecution"
)

func TestClientGetOperation(t *testing.T) {
	testOperation := "testOp1"
	getReq := &longrunning.GetOperationRequest{Name: testOperation}

	eomAsPBAny, err := marshalAny(&remoteexecution.ExecuteOperationMetadata{})
	if err != nil {
		t.Fatalf("Failed to marshal: %s", err)
	}
	resAsPBAny, err := marshalAny(&remoteexecution.ExecuteResponse{})
	if err != nil {
		t.Fatalf("Failed to marshal: %s", err)
	}

	opRes := &longrunning.Operation{
		Name:     testOperation,
		Metadata: eomAsPBAny,
		Done:     true,
		Result: &longrunning.Operation_Response{
			Response: resAsPBAny,
		},
	}

	mockCtrl := gomock.NewController(t)
	opClientMock := mock_longrunning.NewMockOperationsClient(mockCtrl)
	opClientMock.EXPECT().GetOperation(context.Background(), getReq).Return(opRes, nil)

	op, err := getFromClient(opClientMock, getReq)
	if err != nil {
		t.Fatalf("Error on GetOperation: %s", err)
	}

	_, _, _, err = ParseExecuteOperation(op)
	if err != nil {
		t.Fatalf("Error parsing resulting Operation: %s", err)
	}
}

func TestClientCancelOperation(t *testing.T) {
	testOperation := "testOp1"
	cancelReq := &longrunning.CancelOperationRequest{Name: testOperation}

	mockCtrl := gomock.NewController(t)
	opClientMock := mock_longrunning.NewMockOperationsClient(mockCtrl)
	opClientMock.EXPECT().CancelOperation(context.Background(), cancelReq).Return(&empty.Empty{}, nil)

	_, err := cancelFromClient(opClientMock, cancelReq)
	if err != nil {
		t.Fatalf("Error on CancelOperation: %s", err)
	}
}

func TestClientExecute(t *testing.T) {
	req := &remoteexecution.ExecuteRequest{}

	eomAsPBAny, err := marshalAny(&remoteexecution.ExecuteOperationMetadata{})
	if err != nil {
		t.Fatalf("Failed to marshal: %s", err)
	}
	resAsPBAny, err := marshalAny(&remoteexecution.ExecuteResponse{})
	if err != nil {
		t.Fatalf("Failed to marshal: %s", err)
	}

	opRes := &longrunning.Operation{
		Name:     "testOp1",
		Metadata: eomAsPBAny,
		Done:     true,
		Result: &longrunning.Operation_Response{
			Response: resAsPBAny,
		},
	}

	fakeClient := &fakeExecClient{static: opRes}

	mockCtrl := gomock.NewController(t)
	execClientMock := mock_remoteexecution.NewMockExecutionClient(mockCtrl)
	execClientMock.EXPECT().Execute(context.Background(), req).Return(fakeClient, nil)

	op, err := execFromClient(execClientMock, req)
	if err != nil {
		t.Fatalf("Error on Execute: %s", err)
	}

	_, _, _, err = ParseExecuteOperation(op)
	if err != nil {
		t.Fatalf("Error parsing resulting Operation: %s", err)
	}
}

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

// Fake Execution_ExecuteClient
// Implements Execution_ExecuteClient interface
type fakeExecClient struct {
	grpc.ClientStream
	static *longrunning.Operation
}

func (c *fakeExecClient) Recv() (*longrunning.Operation, error) {
	return c.static, nil
}

func (c *fakeExecClient) CloseSend() error {
	return nil
}
