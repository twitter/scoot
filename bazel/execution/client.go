package execution

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/bazel/remoteexecution"
	"github.com/twitter/scoot/common/dialer"
)

// Google Longrunning client APIs

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

// Make a CancelOperation request against a server support the google.longrunning.operations API
// Takes a Resolver and name of the Operation to Cancel
func CancelOperation(r dialer.Resolver, name string) (*empty.Empty, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &longrunning.CancelOperationRequest{Name: name}

	opc := longrunning.NewOperationsClient(cc)
	return cancelFromClient(opc, req)
}

func cancelFromClient(opc longrunning.OperationsClient, req *longrunning.CancelOperationRequest) (*empty.Empty, error) {
	return opc.CancelOperation(context.Background(), req)
}

// Google Bazel Execution client APIs

// Makes an Execute request against a server supporting the google.devtools.remoteexecution API
// Takes a Resolver and ExecuteRequest data. Currently supported:
// * ActionDigest
// * SkipCache
// TBD support for:
// * Misc specs: InstanceName, Policies
func Execute(r dialer.Resolver, actionDigest *remoteexecution.Digest, skipCache bool) (*longrunning.Operation, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &remoteexecution.ExecuteRequest{
		ActionDigest:    actionDigest,
		SkipCacheLookup: skipCache,
	}

	ec := remoteexecution.NewExecutionClient(cc)
	return execFromClient(ec, req)
}

// By convention, execute client recieves first Operation from stream and closes
func execFromClient(ec remoteexecution.ExecutionClient, req *remoteexecution.ExecuteRequest) (*longrunning.Operation, error) {
	execClient, err := ec.Execute(context.Background(), req)
	if err != nil {
		return nil, err
	}
	defer execClient.CloseSend()

	op, err := execClient.Recv()
	if err != nil {
		return nil, err
	}

	return op, nil
}
