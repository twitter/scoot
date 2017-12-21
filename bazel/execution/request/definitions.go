// Execution Request Definitions
// Domain structures for tracking ExecuteRequest info through Scheduler JobDefs and
// Worker RunCommands, and conversions for internal thrift APIs.
package request

import (
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/sched/gen-go/schedthrift"
)

// This type gives us a single reference point for passing Execute Requests and
// leaves room for internal modifications if required
type ExecuteRequest struct {
	Request remoteexecution.ExecuteRequest
}

// Transform schedthrift Bazel ExecuteRequest data into a domain object
func SchedMakeDomainFromThrift(thriftRequest *schedthrift.BazelExecuteRequest) *ExecuteRequest {
	if thriftRequest == nil {
		return nil
	}
	er := remoteexecution.ExecuteRequest{}
	er.InstanceName = thriftRequest.GetInstanceName()
	er.SkipCacheLookup = thriftRequest.GetSkipCache()
	er.Action = schedMakeActionFromThrift(thriftRequest.GetAction())
	return &ExecuteRequest{Request: er}
}

func schedMakeActionFromThrift(thriftAction *schedthrift.BazelAction) *remoteexecution.Action {
	if thriftAction == nil {
		return nil
	}
	d := scootproto.GetDurationFromMs(thriftAction.GetTimeoutMs())
	return &remoteexecution.Action{
		OutputFiles:       thriftAction.GetOutputFiles(),
		OutputDirectories: thriftAction.GetOutputDirs(),
		DoNotCache:        thriftAction.GetNoCache(),
		CommandDigest:     schedMakeDigestFromThrift(thriftAction.GetCommandDigest()),
		InputRootDigest:   schedMakeDigestFromThrift(thriftAction.GetInputDigest()),
		Timeout:           &d,
		Platform:          schedMakePlatformFromThrift(thriftAction.GetPlatformProperties()),
	}
}

func schedMakeDigestFromThrift(thriftDigest *schedthrift.BazelDigest) *remoteexecution.Digest {
	if thriftDigest == nil {
		return nil
	}
	return &remoteexecution.Digest{Hash: thriftDigest.GetHash(), SizeBytes: thriftDigest.GetSizeBytes()}
}

func schedMakePlatformFromThrift(thriftProperties []*schedthrift.BazelProperty) *remoteexecution.Platform {
	platform := &remoteexecution.Platform{}
	platform.Properties = make([]*remoteexecution.Platform_Property, len(thriftProperties))
	for _, prop := range thriftProperties {
		if prop == nil {
			continue
		}
		p := &remoteexecution.Platform_Property{Name: prop.GetName(), Value: prop.GetValue()}
		platform.Properties = append(platform.Properties, p)
	}
	return platform
}

// Transforms domain ExecuteRequest object into schedthrift representation
func SchedMakeThriftFromDomain(executeRequest *ExecuteRequest) *schedthrift.BazelExecuteRequest {
	if executeRequest == nil {
		return nil
	}
	return &schedthrift.BazelExecuteRequest{
		InstanceName: &executeRequest.Request.InstanceName,
		SkipCache:    &executeRequest.Request.SkipCacheLookup,
		Action:       schedMakeActionThriftFromDomain(executeRequest.Request.GetAction()),
	}
}

func schedMakeActionThriftFromDomain(action *remoteexecution.Action) *schedthrift.BazelAction {
	if action == nil {
		return nil
	}
	t := scootproto.GetMsFromDuration(action.GetTimeout())
	return &schedthrift.BazelAction{
		CommandDigest:      schedMakeDigestThriftFromDomain(action.GetCommandDigest()),
		InputDigest:        schedMakeDigestThriftFromDomain(action.GetInputRootDigest()),
		OutputFiles:        action.GetOutputFiles(),
		OutputDirs:         action.GetOutputDirectories(),
		PlatformProperties: schedMakePropertiesThriftFromDomain(action.GetPlatform()),
		TimeoutMs:          &t,
		NoCache:            &action.DoNotCache,
	}
}

func schedMakeDigestThriftFromDomain(digest *remoteexecution.Digest) *schedthrift.BazelDigest {
	if digest == nil {
		return nil
	}
	return &schedthrift.BazelDigest{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()}
}

func schedMakePropertiesThriftFromDomain(platform *remoteexecution.Platform) []*schedthrift.BazelProperty {
	if platform == nil {
		return make([]*schedthrift.BazelProperty, 0)
	}
	props := make([]*schedthrift.BazelProperty, len(platform.GetProperties()))
	for _, p := range platform.GetProperties() {
		if p == nil {
			continue
		}
		bp := &schedthrift.BazelProperty{Name: p.GetName(), Value: p.GetValue()}
		props = append(props, bp)
	}
	return props
}

// WorkerAPI functions
