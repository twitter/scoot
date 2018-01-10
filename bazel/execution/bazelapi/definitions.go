// Execution Request Definitions
// Domain structures for tracking ExecuteRequest info (use cases include Scheduler JobDefs and
// Worker RunCommands), and conversions for internal thrift APIs.
package bazelapi

import (
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	bazelthrift "github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/bazel"
	scootproto "github.com/twitter/scoot/common/proto"
)

// These types give us single reference points for passing Execute Requests and Action Results,
// and leaves room for internal modifications if required
type ExecuteRequest struct {
	Request remoteexecution.ExecuteRequest
}

type ActionResult struct {
	Result remoteexecution.ActionResult
}

func (e *ExecuteRequest) String() string {
	if e == nil {
		return ""
	}
	return e.Request.String()
}

func (a *ActionResult) String() string {
	if a == nil {
		return ""
	}
	return a.Result.String()
}

// Transform request Bazel ExecuteRequest data into a domain object
func MakeExecReqDomainFromThrift(thriftRequest *bazelthrift.ExecuteRequest) *ExecuteRequest {
	if thriftRequest == nil {
		return nil
	}
	er := remoteexecution.ExecuteRequest{}
	er.InstanceName = thriftRequest.GetInstanceName()
	er.SkipCacheLookup = thriftRequest.GetSkipCache()
	er.Action = makeActionFromThrift(thriftRequest.GetAction())
	return &ExecuteRequest{Request: er}
}

// Transform domain ExecuteRequest object into request representation
func MakeExecReqThriftFromDomain(executeRequest *ExecuteRequest) *bazelthrift.ExecuteRequest {
	if executeRequest == nil {
		return nil
	}
	return &bazelthrift.ExecuteRequest{
		InstanceName: &executeRequest.Request.InstanceName,
		SkipCache:    &executeRequest.Request.SkipCacheLookup,
		Action:       makeActionThriftFromDomain(executeRequest.Request.GetAction()),
	}
}

// Transform thrift Bazel ActionResult data into a domain object
func MakeActionResultDomainFromThrift(thriftResult *bazelthrift.ActionResult_) *ActionResult {
	if thriftResult == nil {
		return nil
	}
	ar := remoteexecution.ActionResult{}
	ar.StdoutDigest = makeDigestFromThrift(thriftResult.GetStdoutDigest())
	ar.StderrDigest = makeDigestFromThrift(thriftResult.GetStderrDigest())
	ar.StdoutRaw = thriftResult.GetStdoutRaw()
	ar.StderrRaw = thriftResult.GetStderrRaw()
	ar.OutputFiles = makeOutputFilesFromThrift(thriftResult.GetOutputFiles())
	ar.OutputDirectories = makeOutputDirsFromThrift(thriftResult.GetOutputDirectories())
	ar.ExitCode = thriftResult.GetExitCode()

	return &ActionResult{Result: ar}
}

// Transform domain ActionResult object into thrift representation
func MakeActionResultThriftFromDomain(actionResult *ActionResult) *bazelthrift.ActionResult_ {
	if actionResult == nil {
		return nil
	}
	return &bazelthrift.ActionResult_{
		StdoutDigest:      makeDigestThriftFromDomain(actionResult.Result.StdoutDigest),
		StderrDigest:      makeDigestThriftFromDomain(actionResult.Result.StderrDigest),
		StdoutRaw:         actionResult.Result.StdoutRaw,
		StderrRaw:         actionResult.Result.StderrRaw,
		OutputFiles:       makeOutputFilesThriftFromDomain(actionResult.Result.OutputFiles),
		OutputDirectories: makeOutputDirsThriftFromDomain(actionResult.Result.OutputDirectories),
		ExitCode:          &actionResult.Result.ExitCode,
	}
}

// From thrift to domain translations
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

// From domain to thrift translations

func makeActionThriftFromDomain(action *remoteexecution.Action) *bazelthrift.Action {
	if action == nil {
		return nil
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
	return &bazelthrift.Digest{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()}
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
		bp := &bazelthrift.Property{Name: p.GetName(), Value: p.GetValue()}
		props = append(props, bp)
	}
	return props
}
