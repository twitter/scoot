package execution

// To generate required mockgen files for these tests, from the Top Level github.com/twitter/scoot dir:
//
//  (prerequisite: go get google.golang.org/genproto/googleapis/longrunning)
//  mockgen google.golang.org/genproto/googleapis/longrunning OperationsClient > bazel/execution/mock_longrunning/opclient_mock.go
//	NOTE: in the generated file, replace the "context" import with "golang.org/x/net/context"
//	this seems to be a go version/mock incompatability
//
//  mockgen -source=/Users/dgassaway/workspace/src/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test/remote_execution.pb.go ExecutionClient > bazel/execution/mock_remoteexecution/execclient_mock.go
// NOTE: in the generated file, add the following line to the import list:
//	. "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
// this is due to a longstanding gomock bug the owners are sitting on: https://github.com/golang/mock/issues/77
//

import (
	"testing"

	"github.com/golang/mock/gomock"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"

	"github.com/twitter/scoot/bazel/execution/mock_longrunning"
	"github.com/twitter/scoot/bazel/execution/mock_remoteexecution"
)

func TestGetOperation(t *testing.T) {
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

	_, _, err = ParseExecuteOperation(op)
	if err != nil {
		t.Fatalf("Error parsing resulting Operation: %s", err)
	}
}

func TestExecute(t *testing.T) {
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

	mockCtrl := gomock.NewController(t)
	execClientMock := mock_remoteexecution.NewMockExecutionClient(mockCtrl)
	execClientMock.EXPECT().Execute(context.Background(), req).Return(opRes, nil)

	op, err := execFromClient(execClientMock, req)
	if err != nil {
		t.Fatalf("Error on Execute: %s", err)
	}

	_, _, err = ParseExecuteOperation(op)
	if err != nil {
		t.Fatalf("Error parsing resulting Operation: %s", err)
	}
}
