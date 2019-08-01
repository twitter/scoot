package execution

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/proto"
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

// Internal, util client functions

// Parse a generic longrunning.Operation structure into expected Execution API components.
// Deserializes Operation.Metadata as an ExecuteOperationMetadata always.
// Checks for Operation.Result as a Response and if found deserializes as an ExecuteResponse.
// Per Bazel API, does not check for Operation.Result as a GRPC Status/Error - not allowed.
func ParseExecuteOperation(op *longrunning.Operation) (*remoteexecution.ExecuteOperationMetadata,
	*remoteexecution.ExecuteResponse, error) {
	if op == nil {
		return nil, nil, fmt.Errorf("Attempting to parse nil Operation")
	}

	eom := &remoteexecution.ExecuteOperationMetadata{}
	err := ptypes.UnmarshalAny(op.GetMetadata(), eom)
	if err != nil {
		return nil, nil, fmt.Errorf("Error deserializing metadata as ExecuteOperationMetadata: %s", err)
	}

	if op.GetResponse() == nil {
		return eom, nil, nil
	}

	res := &remoteexecution.ExecuteResponse{}
	err = ptypes.UnmarshalAny(op.GetResponse(), res)
	if err != nil {
		return nil, nil, fmt.Errorf("Error deserializing response as ExecuteResponse: %s", err)
	}
	return eom, res, nil
}

// JSON unmarshalling doesn't work for Operations with nested Results, as they're of unexported type isOperation_Result.
// Thus, we need custom unmarshalling logic.
func ExtractOpFromJson(opBytes []byte) (*longrunning.Operation, error) {
	var opMapTemplate interface{}
	err := json.Unmarshal(opBytes, &opMapTemplate)
	if err != nil {
		return nil, err
	}
	opMap := opMapTemplate.(map[string]interface{})
	op := &longrunning.Operation{}
	for key, val := range opMap {
		switch key {
		case "name":
			if name, ok := val.(string); !ok {
				return nil, deserializeErr("name", "string", val)
			} else {
				op.Name = name
			}
		case "metadata":
			metadata := &any.Any{}
			if metadataMap, ok := val.(map[string]interface{}); ok {
				for mKey, mVal := range metadataMap {
					switch mKey {
					case "type_url":
						if typeUrl, ok := mVal.(string); !ok {
							return nil, deserializeErr("type_url", "string", mVal)
						} else {
							metadata.TypeUrl = typeUrl
						}
					case "value":
						if value, ok := mVal.(string); !ok {
							return nil, deserializeErr("value", "string", mVal)
						} else {
							metadata.Value = []byte(value)
						}
					}
				}
				op.Metadata = metadata
			}
		case "done":
			if done, ok := val.(bool); !ok {
				return nil, deserializeErr("done", "bool", val)
			} else {
				op.Done = done
			}
		case "Result":
			if opResultMap, ok := val.(map[string]interface{}); ok {
				for resultKey, resultVal := range opResultMap {
					switch resultKey {
					case "Error":
						opErrorStatus := &status.Status{}
						opError := &longrunning.Operation_Error{
							Error: opErrorStatus,
						}
						op.Result = opError
						if errorMap, ok := resultVal.(map[string]interface{}); ok {
							for errKey, errVal := range errorMap {
								switch errKey {
								case "code":
									if code, ok := errVal.(float64); !ok {
										return nil, deserializeErr("code", "float64", errVal)
									} else {
										opErrorStatus.Code = int32(code)
									}
								case "message":
									if message, ok := errVal.(string); !ok {
										return nil, deserializeErr("message", "string", errVal)
									} else {
										opErrorStatus.Message = message
									}
								}
							}
						}
					case "Response":
						opResponseAny := &any.Any{}
						opResponse := &longrunning.Operation_Response{
							Response: opResponseAny,
						}
						op.Result = opResponse
						if responseMap, ok := resultVal.(map[string]interface{}); ok {
							for responseKey, responseVal := range responseMap {
								switch responseKey {
								case "type_url":
									if typeUrl, ok := responseVal.(string); !ok {
										return nil, deserializeErr("type_url", "string", responseVal)
									} else {
										opResponseAny.TypeUrl = typeUrl
									}
								case "value":
									if value, ok := responseVal.(string); !ok {
										return nil, deserializeErr("value", "string", responseVal)
									} else {
										opResponseAny.Value = []byte(value)
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return op, nil
}

// Create error for failing to deserialize a field as an expected type
func deserializeErr(keyName, expectedType string, value interface{}) error {
	return fmt.Errorf("value for key '%s' was not of type %s. %v: %s", keyName, expectedType, value, reflect.TypeOf(value))
}

// String conversion for human consumption of Operation's nested data
func ExecuteOperationToStr(op *longrunning.Operation) string {
	eom, res, err := ParseExecuteOperation(op)
	if err != nil {
		return ""
	}
	s := fmt.Sprintf("Operation: %s\n\tDone: %t\n", op.GetName(), op.GetDone())
	s += fmt.Sprintf("\tMetadata:\n")
	s += fmt.Sprintf("\t\tStage: %s\n", eom.GetStage())
	s += fmt.Sprintf("\t\tActionDigest: %s\n", bazel.DigestToStr(eom.GetActionDigest()))
	if res != nil {
		s += fmt.Sprintf("\tExecResponse:\n")
		s += fmt.Sprintf("\t\tStatus: %s\n", res.GetStatus())
		s += fmt.Sprintf("\t\t\tCode: %s\n", code.Code_name[res.GetStatus().GetCode()])
		s += fmt.Sprintf("\t\tCached: %t\n", res.GetCachedResult())
		s += fmt.Sprintf("\t\tActionResult:\n")
		s += fmt.Sprintf("\t\t\tExitCode: %d\n", res.GetResult().GetExitCode())
		s += fmt.Sprintf("\t\t\tOutputFiles: %s\n", res.GetResult().GetOutputFiles())
		s += fmt.Sprintf("\t\t\tOutputDirectories: %s\n", res.GetResult().GetOutputDirectories())
		s += fmt.Sprintf("\t\t\tStdoutDigest: %s\n", bazel.DigestToStr(res.GetResult().GetStdoutDigest()))
		s += fmt.Sprintf("\t\t\tStderrDigest: %s\n", bazel.DigestToStr(res.GetResult().GetStderrDigest()))
		if res.GetResult().GetExecutionMetadata() != nil {
			em := res.GetResult().GetExecutionMetadata()
			s += fmt.Sprintf("\t\t\tExecutionMetadata:\n")
			s += fmt.Sprintf("\t\t\t\tWorker: %s\n", em.GetWorker())
			s = addLatencyToStr(s, "\t\t\t\t", "QueueLatency", em.GetQueuedTimestamp(), em.GetWorkerStartTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "WorkerTotal", em.GetWorkerStartTimestamp(), em.GetWorkerCompletedTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "InputFetch", em.GetInputFetchStartTimestamp(), em.GetInputFetchCompletedTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "Execution", em.GetExecutionStartTimestamp(), em.GetExecutionCompletedTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "OutputUpload", em.GetOutputUploadStartTimestamp(), em.GetOutputUploadCompletedTimestamp())
		}
	}
	return s
}

func addLatencyToStr(inputStr, indent, label string, startTs, endTs *timestamp.Timestamp) string {
	if startTs == nil || endTs == nil {
		return inputStr
	}
	startTime, endTime := proto.GetTimeFromTimestamp(startTs), proto.GetTimeFromTimestamp(endTs)
	duration := endTime.Sub(startTime)
	ms := duration.Nanoseconds() / int64(time.Millisecond)
	inputStr += fmt.Sprintf("%s%s: %dms\n", indent, label, ms)
	return inputStr
}
