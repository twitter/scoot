// Execution Request & Action Result Definitions
// Domain structures for tracking ExecuteRequest info (use cases include Scheduler JobDefs and
// Worker RunCommands), ActionResult info (used in Worker RunStatuses), and conversions for
// internal thrift APIs.
package bazelapi

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	log "github.com/sirupsen/logrus"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"

	bazelthrift "github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/bazel"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
)

const PreconditionMissing = "MISSING"

// These types give us single reference points for passing Execute Requests and Action Results

// Add ExecutionMetadata so metadata added in the scheduling phase is passed through to worker
// Not Passed Through Thrift:
// Add Action so worker has a place to store after fetching during invoke
// Add Command so worker has a place to store after fetching during invoke
type ExecuteRequest struct {
	Request           *remoteexecution.ExecuteRequest
	ExecutionMetadata *remoteexecution.ExecutedActionMetadata
	Action            *remoteexecution.Action
	Command           *remoteexecution.Command
}

// Add ActionDigest again here so it's available when polling status - no ref to original request
// Add GoogleAPIs RPC Status here so that we can propagate detailed error statuses from the runner upwards
// Add Cached here so that the runner can propagate whether it used a cached result upwards
type ActionResult struct {
	Result       *remoteexecution.ActionResult
	ActionDigest *remoteexecution.Digest
	GRPCStatus   *google_rpc_status.Status
	Cached       bool
}

func (e *ExecuteRequest) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s", e.Request)
}

func (e *ExecuteRequest) GetRequest() *remoteexecution.ExecuteRequest {
	if e == nil {
		return &remoteexecution.ExecuteRequest{}
	}
	return e.Request
}

func (e *ExecuteRequest) GetExecutionMetadata() *remoteexecution.ExecutedActionMetadata {
	if e == nil {
		return &remoteexecution.ExecutedActionMetadata{}
	}
	return e.ExecutionMetadata
}

// Not exported to thrift
func (e *ExecuteRequest) GetAction() *remoteexecution.Action {
	if e == nil {
		return &remoteexecution.Action{}
	}
	return e.Action
}

// Not exported to thrift
func (e *ExecuteRequest) GetCommand() *remoteexecution.Command {
	if e == nil {
		return &remoteexecution.Command{}
	}
	return e.Command
}

func (a *ActionResult) String() string {
	if a == nil {
		return ""
	}
	return fmt.Sprintf("%s\n%s\n%s", a.Result, a.ActionDigest, a.GRPCStatus)
}

func (a *ActionResult) GetResult() *remoteexecution.ActionResult {
	if a == nil {
		return &remoteexecution.ActionResult{}
	}
	return a.Result
}

func (a *ActionResult) GetActionDigest() *remoteexecution.Digest {
	if a == nil {
		return &remoteexecution.Digest{}
	}
	return a.ActionDigest
}

func (a *ActionResult) GetGRPCStatus() *google_rpc_status.Status {
	if a == nil {
		return &google_rpc_status.Status{}
	}
	return a.GRPCStatus
}

func (a *ActionResult) GetCached() bool {
	if a == nil {
		return false
	}
	return a.Cached
}

// Transform request Bazel ExecuteRequest data into a domain object
func MakeExecReqDomainFromThrift(thriftRequest *bazelthrift.ExecuteRequest) *ExecuteRequest {
	if thriftRequest == nil {
		return nil
	}
	er := &remoteexecution.ExecuteRequest{
		InstanceName:       thriftRequest.GetInstanceName(),
		SkipCacheLookup:    thriftRequest.GetSkipCache(),
		ActionDigest:       makeDigestFromThrift(thriftRequest.GetActionDigest()),
		ExecutionPolicy:    makeExecPolicyFromThrift(thriftRequest.GetExecutionPolicy()),
		ResultsCachePolicy: makeResCachePolicyFromThrift(thriftRequest.GetResultsCachePolicy()),
	}
	return &ExecuteRequest{
		Request:           er,
		ExecutionMetadata: makeExecutionMetadataFromThrift(thriftRequest.GetExecutionMetadata()),
	}
}

// Transform domain ExecuteRequest object into request representation
func MakeExecReqThriftFromDomain(executeRequest *ExecuteRequest) *bazelthrift.ExecuteRequest {
	// Return nil if domain is nil is OK since this is a top-level data structure
	if executeRequest == nil {
		return nil
	}
	return &bazelthrift.ExecuteRequest{
		InstanceName:       &executeRequest.Request.InstanceName,
		SkipCache:          &executeRequest.Request.SkipCacheLookup,
		ActionDigest:       makeDigestThriftFromDomain(executeRequest.Request.GetActionDigest()),
		ExecutionPolicy:    makeExecPolicyThriftFromDomain(executeRequest.Request.GetExecutionPolicy()),
		ResultsCachePolicy: makeResCachePolicyThriftFromDomain(executeRequest.Request.GetResultsCachePolicy()),
		ExecutionMetadata:  makeExecutionMetadataThriftFromDomain(executeRequest.ExecutionMetadata),
	}
}

// Transform thrift Bazel ActionResult data into a domain object
func MakeActionResultDomainFromThrift(thriftResult *bazelthrift.ActionResult_) *ActionResult {
	if thriftResult == nil {
		return nil
	}
	ar := &remoteexecution.ActionResult{
		OutputFiles:       makeOutputFilesFromThrift(thriftResult.GetOutputFiles()),
		OutputDirectories: makeOutputDirsFromThrift(thriftResult.GetOutputDirectories()),
		ExitCode:          thriftResult.GetExitCode(),
		StdoutRaw:         thriftResult.GetStdoutRaw(),
		StdoutDigest:      makeDigestFromThrift(thriftResult.GetStdoutDigest()),
		StderrRaw:         thriftResult.GetStderrRaw(),
		StderrDigest:      makeDigestFromThrift(thriftResult.GetStderrDigest()),
		ExecutionMetadata: makeExecutionMetadataFromThrift(thriftResult.GetExecutionMetadata()),
	}
	return &ActionResult{
		Result:       ar,
		ActionDigest: makeDigestFromThrift(thriftResult.GetActionDigest()),
		GRPCStatus:   makeGRPCStatusFromThrift(thriftResult.GetGRPCStatus()),
		Cached:       thriftResult.GetCached(),
	}
}

// Transform domain ActionResult object into thrift representation
func MakeActionResultThriftFromDomain(actionResult *ActionResult) *bazelthrift.ActionResult_ {
	// Return nil if domain is nil is OK since this is a top-level data structure
	if actionResult == nil {
		return nil
	}
	var ec int32 = actionResult.Result.GetExitCode()
	var cached bool = actionResult.GetCached()
	return &bazelthrift.ActionResult_{
		OutputFiles:       makeOutputFilesThriftFromDomain(actionResult.Result.GetOutputFiles()),
		OutputDirectories: makeOutputDirsThriftFromDomain(actionResult.Result.GetOutputDirectories()),
		ExitCode:          &ec,
		StdoutRaw:         actionResult.Result.GetStdoutRaw(),
		StdoutDigest:      makeDigestThriftFromDomain(actionResult.Result.GetStdoutDigest()),
		StderrRaw:         actionResult.Result.GetStderrRaw(),
		StderrDigest:      makeDigestThriftFromDomain(actionResult.Result.GetStderrDigest()),
		ExecutionMetadata: makeExecutionMetadataThriftFromDomain(actionResult.Result.GetExecutionMetadata()),
		ActionDigest:      makeDigestThriftFromDomain(actionResult.ActionDigest),
		GRPCStatus:        makeGRPCStatusThriftFromDomain(actionResult.GRPCStatus),
		Cached:            &cached,
	}
}

// Unexported "from thrift to domain" translations

func makeDigestFromThrift(thriftDigest *bazelthrift.Digest) *remoteexecution.Digest {
	if thriftDigest == nil {
		return nil
	}
	return &remoteexecution.Digest{Hash: thriftDigest.GetHash(), SizeBytes: thriftDigest.GetSizeBytes()}
}

func makeOutputDirFromThrift(outputDir *bazelthrift.OutputDirectory) *remoteexecution.OutputDirectory {
	if outputDir == nil {
		return nil
	}
	return &remoteexecution.OutputDirectory{
		Path:       outputDir.GetPath(),
		TreeDigest: makeDigestFromThrift(outputDir.GetTreeDigest()),
	}
}

func makeOutputDirsFromThrift(outputDirs []*bazelthrift.OutputDirectory) []*remoteexecution.OutputDirectory {
	ods := []*remoteexecution.OutputDirectory{}
	for _, d := range outputDirs {
		ods = append(ods, makeOutputDirFromThrift(d))
	}
	return ods
}

func makeOutputFileFromThrift(outputFile *bazelthrift.OutputFile) *remoteexecution.OutputFile {
	if outputFile == nil {
		return nil
	}
	return &remoteexecution.OutputFile{
		Path:         outputFile.GetPath(),
		Digest:       makeDigestFromThrift(outputFile.GetDigest()),
		IsExecutable: outputFile.GetIsExecutable(),
	}
}

func makeOutputFilesFromThrift(outputFiles []*bazelthrift.OutputFile) []*remoteexecution.OutputFile {
	ofs := []*remoteexecution.OutputFile{}
	for _, f := range outputFiles {
		ofs = append(ofs, makeOutputFileFromThrift(f))
	}
	return ofs
}

// returns nil and logs on error
func makeGRPCStatusFromThrift(asBytes []byte) *google_rpc_status.Status {
	if asBytes == nil {
		return nil
	}
	status := &google_rpc_status.Status{}
	err := proto.Unmarshal(asBytes, status)
	if err != nil {
		log.Errorf("Failed to deserialize thrift to google rpc status: %s", err)
		return nil
	}
	return status
}

func makeTimestampFromThrift(ts *bazelthrift.Timestamp) *timestamp.Timestamp {
	if ts == nil {
		return nil
	}
	return &timestamp.Timestamp{Seconds: ts.GetSeconds(), Nanos: ts.GetNanos()}
}

func makeExecutionMetadataFromThrift(em *bazelthrift.ExecutedActionMetadata) *remoteexecution.ExecutedActionMetadata {
	if em == nil {
		return nil
	}
	return &remoteexecution.ExecutedActionMetadata{
		Worker:                         em.GetWorker(),
		QueuedTimestamp:                makeTimestampFromThrift(em.GetQueuedTimestamp()),
		WorkerStartTimestamp:           makeTimestampFromThrift(em.GetWorkerStartTimestamp()),
		WorkerCompletedTimestamp:       makeTimestampFromThrift(em.GetWorkerCompletedTimestamp()),
		InputFetchStartTimestamp:       makeTimestampFromThrift(em.GetInputFetchStartTimestamp()),
		InputFetchCompletedTimestamp:   makeTimestampFromThrift(em.GetInputFetchCompletedTimestamp()),
		ExecutionStartTimestamp:        makeTimestampFromThrift(em.GetExecutionStartTimestamp()),
		ExecutionCompletedTimestamp:    makeTimestampFromThrift(em.GetExecutionCompletedTimestamp()),
		OutputUploadStartTimestamp:     makeTimestampFromThrift(em.GetOutputUploadStartTimestamp()),
		OutputUploadCompletedTimestamp: makeTimestampFromThrift(em.GetOutputUploadCompletedTimestamp()),
	}
}

func makeExecPolicyFromThrift(policy *bazelthrift.ExecutionPolicy) *remoteexecution.ExecutionPolicy {
	if policy == nil {
		return nil
	}
	return &remoteexecution.ExecutionPolicy{Priority: policy.GetPriority()}
}

func makeResCachePolicyFromThrift(policy *bazelthrift.ResultsCachePolicy) *remoteexecution.ResultsCachePolicy {
	if policy == nil {
		return nil
	}
	return &remoteexecution.ResultsCachePolicy{Priority: policy.GetPriority()}
}

// Unexported "from domain to thrift" translations

func makeDigestThriftFromDomain(digest *remoteexecution.Digest) *bazelthrift.Digest {
	if digest == nil {
		return nil
	}
	var hash string = digest.GetHash()
	var size int64 = digest.GetSizeBytes()
	return &bazelthrift.Digest{Hash: &hash, SizeBytes: &size}
}

func makeOutputDirThriftFromDomain(outputDir *remoteexecution.OutputDirectory) *bazelthrift.OutputDirectory {
	p := outputDir.GetPath()
	return &bazelthrift.OutputDirectory{
		Path:       &p,
		TreeDigest: makeDigestThriftFromDomain(outputDir.GetTreeDigest()),
	}
}

func makeOutputDirsThriftFromDomain(outputDirs []*remoteexecution.OutputDirectory) []*bazelthrift.OutputDirectory {
	ods := []*bazelthrift.OutputDirectory{}
	for _, d := range outputDirs {
		ods = append(ods, makeOutputDirThriftFromDomain(d))
	}
	return ods
}

func makeOutputFileThriftFromDomain(outputFile *remoteexecution.OutputFile) *bazelthrift.OutputFile {
	p := outputFile.GetPath()
	e := outputFile.GetIsExecutable()
	return &bazelthrift.OutputFile{
		Path:         &p,
		Digest:       makeDigestThriftFromDomain(outputFile.GetDigest()),
		IsExecutable: &e,
	}
}

func makeOutputFilesThriftFromDomain(outputFiles []*remoteexecution.OutputFile) []*bazelthrift.OutputFile {
	ofs := []*bazelthrift.OutputFile{}
	for _, f := range outputFiles {
		ofs = append(ofs, makeOutputFileThriftFromDomain(f))
	}
	return ofs
}

// returns nil and logs on error
func makeGRPCStatusThriftFromDomain(status *google_rpc_status.Status) []byte {
	if status == nil {
		return nil
	}
	asBytes, err := proto.Marshal(status)
	if err != nil {
		log.Errorf("Failed to serialize google rpc status for thrift: %s", err)
		return nil
	}
	return asBytes
}

func makeTimestampThriftFromDomain(ts *timestamp.Timestamp) *bazelthrift.Timestamp {
	if ts == nil {
		return nil
	}
	var seconds int64 = ts.GetSeconds()
	var nanos int32 = ts.GetNanos()
	return &bazelthrift.Timestamp{Seconds: &seconds, Nanos: &nanos}
}

func makeExecutionMetadataThriftFromDomain(em *remoteexecution.ExecutedActionMetadata) *bazelthrift.ExecutedActionMetadata {
	if em == nil {
		return nil
	}
	var worker string = em.GetWorker()
	return &bazelthrift.ExecutedActionMetadata{
		Worker:                         &worker,
		QueuedTimestamp:                makeTimestampThriftFromDomain(em.GetQueuedTimestamp()),
		WorkerStartTimestamp:           makeTimestampThriftFromDomain(em.GetWorkerStartTimestamp()),
		WorkerCompletedTimestamp:       makeTimestampThriftFromDomain(em.GetWorkerCompletedTimestamp()),
		InputFetchStartTimestamp:       makeTimestampThriftFromDomain(em.GetInputFetchStartTimestamp()),
		InputFetchCompletedTimestamp:   makeTimestampThriftFromDomain(em.GetInputFetchCompletedTimestamp()),
		ExecutionStartTimestamp:        makeTimestampThriftFromDomain(em.GetExecutionStartTimestamp()),
		ExecutionCompletedTimestamp:    makeTimestampThriftFromDomain(em.GetExecutionCompletedTimestamp()),
		OutputUploadStartTimestamp:     makeTimestampThriftFromDomain(em.GetOutputUploadStartTimestamp()),
		OutputUploadCompletedTimestamp: makeTimestampThriftFromDomain(em.GetOutputUploadCompletedTimestamp()),
	}
}

func makeExecPolicyThriftFromDomain(policy *remoteexecution.ExecutionPolicy) *bazelthrift.ExecutionPolicy {
	if policy == nil {
		return nil
	}
	var p int32 = policy.GetPriority()
	return &bazelthrift.ExecutionPolicy{Priority: &p}
}

func makeResCachePolicyThriftFromDomain(policy *remoteexecution.ResultsCachePolicy) *bazelthrift.ResultsCachePolicy {
	if policy == nil {
		return nil
	}
	var p int32 = policy.GetPriority()
	return &bazelthrift.ResultsCachePolicy{Priority: &p}
}
