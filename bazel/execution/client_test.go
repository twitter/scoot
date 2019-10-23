package execution

//go:generate mockgen -destination=mock_remoteexecution/execclient_mock.go github.com/twitter/scoot/bazel/remoteexecution ExecutionClient,ActionCacheClient,ContentAddressableStorageClient
//go:generate mockgen -destination=mock_longrunning/opclient_mock.go google.golang.org/genproto/googleapis/longrunning OperationsClient

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/bazel/execution/mock_longrunning"
	"github.com/twitter/scoot/bazel/execution/mock_remoteexecution"
	"github.com/twitter/scoot/bazel/remoteexecution"
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
