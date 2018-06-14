// Execution Request & Action Result Definitions
// Domain structures for tracking ExecuteRequest info (use cases include Scheduler JobDefs and
// Worker RunCommands), ActionResult info (used in Worker RunStatuses), and conversions for
// internal thrift APIs.
package bazelapi

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"

	bazelthrift "github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/bazel"
	scootproto "github.com/twitter/scoot/common/proto"
)

const PreconditionMissing = "MISSING"

// These types give us single reference points for passing Execute Requests and Action Results

// Add ActionDigest so it's available throughout the Scoot stack without having to recompute
type ExecuteRequest struct {
	Request      *remoteexecution.ExecuteRequest
	ActionDigest *remoteexecution.Digest
}

// Add ActionDigest again here so it's available when polling status - no ref to original request
// Add GoogleAPIs RPC Status here so that we can propagate detailed error statuses from the runner upwards
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
	return fmt.Sprintf("%s\n%s", e.Request, e.ActionDigest)
}

func (a *ActionResult) String() string {
	if a == nil {
		return ""
	}
	return fmt.Sprintf("%s\n%s\n%s", a.Result, a.ActionDigest, a.GRPCStatus)
}

func (e *ExecuteRequest) GetRequest() *remoteexecution.ExecuteRequest {
	if e == nil {
		return &remoteexecution.ExecuteRequest{}
	}
	return e.Request
}

func (e *ExecuteRequest) GetActionDigest() *remoteexecution.Digest {
	if e == nil {
		return &remoteexecution.Digest{}
	}
	return e.ActionDigest
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
		InstanceName:    thriftRequest.GetInstanceName(),
		SkipCacheLookup: thriftRequest.GetSkipCache(),
		Action:          makeActionFromThrift(thriftRequest.GetAction()),
	}
	return &ExecuteRequest{
		Request:      er,
		ActionDigest: makeDigestFromThrift(thriftRequest.GetActionDigest()),
	}
}

// Transform domain ExecuteRequest object into request representation
func MakeExecReqThriftFromDomain(executeRequest *ExecuteRequest) *bazelthrift.ExecuteRequest {
	// Return nil if domain is nil is OK since this is a top-level data structure
	if executeRequest == nil {
		return nil
	}
	return &bazelthrift.ExecuteRequest{
		InstanceName: &executeRequest.Request.InstanceName,
		SkipCache:    &executeRequest.Request.SkipCacheLookup,
		Action:       makeActionThriftFromDomain(executeRequest.Request.GetAction()),
		ActionDigest: makeDigestThriftFromDomain(executeRequest.ActionDigest),
	}
}

// Transform thrift Bazel ActionResult data into a domain object
func MakeActionResultDomainFromThrift(thriftResult *bazelthrift.ActionResult_) *ActionResult {
	if thriftResult == nil {
		return nil
	}
	ar := &remoteexecution.ActionResult{
		StdoutDigest:      makeDigestFromThrift(thriftResult.GetStdoutDigest()),
		StderrDigest:      makeDigestFromThrift(thriftResult.GetStderrDigest()),
		StdoutRaw:         thriftResult.GetStdoutRaw(),
		StderrRaw:         thriftResult.GetStderrRaw(),
		OutputFiles:       makeOutputFilesFromThrift(thriftResult.GetOutputFiles()),
		OutputDirectories: makeOutputDirsFromThrift(thriftResult.GetOutputDirectories()),
		ExitCode:          thriftResult.GetExitCode(),
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
		StdoutDigest:      makeDigestThriftFromDomain(actionResult.Result.GetStdoutDigest()),
		StderrDigest:      makeDigestThriftFromDomain(actionResult.Result.GetStderrDigest()),
		StdoutRaw:         actionResult.Result.GetStdoutRaw(),
		StderrRaw:         actionResult.Result.GetStderrRaw(),
		OutputFiles:       makeOutputFilesThriftFromDomain(actionResult.Result.GetOutputFiles()),
		OutputDirectories: makeOutputDirsThriftFromDomain(actionResult.Result.GetOutputDirectories()),
		ExitCode:          &ec,
		ActionDigest:      makeDigestThriftFromDomain(actionResult.ActionDigest),
		GRPCStatus:        makeGRPCStatusThriftFromDomain(actionResult.GRPCStatus),
		Cached:            &cached,
	}
}

// Unexported "from thrift to domain" translations
func makeActionFromThrift(thriftAction *bazelthrift.Action) *remoteexecution.Action {
	if thriftAction == nil {
		return nil
	}
	return &remoteexecution.Action{
		OutputFiles:       thriftAction.GetOutputFiles(),
		OutputDirectories: thriftAction.GetOutputDirs(),
		DoNotCache:        thriftAction.GetNoCache(),
		CommandDigest:     makeDigestFromThrift(thriftAction.GetCommandDigest()),
		InputRootDigest:   makeDigestFromThrift(thriftAction.GetInputDigest()),
		Timeout:           scootproto.GetDurationFromMs(thriftAction.GetTimeoutMs()),
		Platform:          makePlatformFromThrift(thriftAction.GetPlatformProperties()),
	}
}

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
		TreeDigest: makeDigestFromThrift(outputDir.GetTreeDigest()),
		Path:       outputDir.GetPath(),
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
		Digest:       makeDigestFromThrift(outputFile.GetDigest()),
		Path:         outputFile.GetPath(),
		Content:      outputFile.GetContent(),
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

func makePlatformFromThrift(thriftProperties []*bazelthrift.Property) *remoteexecution.Platform {
	platform := &remoteexecution.Platform{}
	platform.Properties = make([]*remoteexecution.Platform_Property, 0, len(thriftProperties))
	for _, prop := range thriftProperties {
		if prop == nil {
			continue
		}
		p := &remoteexecution.Platform_Property{Name: prop.GetName(), Value: prop.GetValue()}
		platform.Properties = append(platform.Properties, p)
	}
	return platform
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

// Unexported "from domain to thrift" translations
func makeActionThriftFromDomain(action *remoteexecution.Action) *bazelthrift.Action {
	if action == nil {
		return &bazelthrift.Action{}
	}
	t := scootproto.GetMsFromDuration(action.GetTimeout())
	return &bazelthrift.Action{
		CommandDigest:      makeDigestThriftFromDomain(action.GetCommandDigest()),
		InputDigest:        makeDigestThriftFromDomain(action.GetInputRootDigest()),
		OutputFiles:        action.GetOutputFiles(),
		OutputDirs:         action.GetOutputDirectories(),
		PlatformProperties: makePropertiesThriftFromDomain(action.GetPlatform()),
		TimeoutMs:          &t,
		NoCache:            &action.DoNotCache,
	}
}

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
		TreeDigest: makeDigestThriftFromDomain(outputDir.GetTreeDigest()),
		Path:       &p,
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
		Digest:       makeDigestThriftFromDomain(outputFile.GetDigest()),
		Path:         &p,
		Content:      outputFile.GetContent(),
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

func makePropertiesThriftFromDomain(platform *remoteexecution.Platform) []*bazelthrift.Property {
	if platform == nil {
		return make([]*bazelthrift.Property, 0)
	}
	props := make([]*bazelthrift.Property, 0, len(platform.GetProperties()))
	for _, p := range platform.GetProperties() {
		if p == nil {
			continue
		}
		var name string = p.GetName()
		var value string = p.GetValue()
		bp := &bazelthrift.Property{Name: &name, Value: &value}
		props = append(props, bp)
	}
	return props
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
