package execution

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/common/dialer"
)

// Execution server,Longrunning client APIs

// Make a GetOperation request against a server supporting the google.longrunning.operations API
// Takes a Resolver and name of the Operation to Get
func GetOperation(r dialer.Resolver, name string) (*longrunning.Operation, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &longrunning.GetOperationRequest{Name: name}

	opc := longrunning.NewOperationsClient(cc)
	return getFromClient(opc, req)
}

func getFromClient(opc longrunning.OperationsClient, req *longrunning.GetOperationRequest) (*longrunning.Operation, error) {
	return opc.GetOperation(context.Background(), req)
}

// Parse a generic longrunning.Operation structure into expected Execution API components.
// Deserializes Operation.Metadata as an ExecuteOperationMetadata always
// Checks Operation.Result, which is essentially a union field of either an error or response:
// 	If an Error is found, returns the grpc Status
// 	If a Response is found, deserializes it as an ExecuteResponse
// The caller should assume that between the Status and ExecuteResponse, only one is set
func ParseExecuteOperation(op *longrunning.Operation) (*remoteexecution.ExecuteOperationMetadata,
	*google_rpc_status.Status, *remoteexecution.ExecuteResponse, error) {
	if op == nil {
		return nil, nil, nil, fmt.Errorf("Attempting to parse nil Operation")
	}

	eom := &remoteexecution.ExecuteOperationMetadata{}
	err := ptypes.UnmarshalAny(op.GetMetadata(), eom)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Error deserializing metadata as ExecuteOperationMetadata: %s", err)
	}

	s := op.GetError()
	if s != nil {
		return eom, s, nil, nil
	}

	res := &remoteexecution.ExecuteResponse{}
	err = ptypes.UnmarshalAny(op.GetResponse(), res)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Error deserializing response as ExecuteResponse: %s", err)
	}
	return eom, nil, res, nil
}

func ExecuteOperationToStr(op *longrunning.Operation) string {
	eom, st, res, err := ParseExecuteOperation(op)
	if err != nil {
		return ""
	}
	scs := spew.NewDefaultConfig()
	scs.Indent = "\t"
	return fmt.Sprintf("(*Operation.Name)(%s)\n(*Operation.Done)(%t)\n%s", op.GetName(), op.GetDone(), spew.Sdump(eom, st, res))
}
