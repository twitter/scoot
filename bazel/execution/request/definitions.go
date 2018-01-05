// Execution Request Definitions
// Domain structures for tracking ExecuteRequest info through Scheduler JobDefs and
// Worker RunCommands, and conversions for internal thrift APIs.
package request

import (
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel/execution/request/gen-go/request"
	scootproto "github.com/twitter/scoot/common/proto"
)

// This type gives us a single reference point for passing Execute Requests and
// leaves room for internal modifications if required
type ExecuteRequest struct {
	Request remoteexecution.ExecuteRequest
}

// TODO rename sched???
// Transform request Bazel ExecuteRequest data into a domain object
func SchedMakeDomainFromThrift(thriftRequest *request.BazelExecuteRequest) *ExecuteRequest {
	if thriftRequest == nil {
		return nil
	}
	er := remoteexecution.ExecuteRequest{}
	er.InstanceName = thriftRequest.GetInstanceName()
	er.SkipCacheLookup = thriftRequest.GetSkipCache()
	er.Action = schedMakeActionFromThrift(thriftRequest.GetAction())
	return &ExecuteRequest{Request: er}
}

func schedMakeActionFromThrift(thriftAction *request.BazelAction) *remoteexecution.Action {
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

func schedMakeDigestFromThrift(thriftDigest *request.BazelDigest) *remoteexecution.Digest {
	if thriftDigest == nil {
		return nil
	}
	return &remoteexecution.Digest{Hash: thriftDigest.GetHash(), SizeBytes: thriftDigest.GetSizeBytes()}
}

func schedMakePlatformFromThrift(thriftProperties []*request.BazelProperty) *remoteexecution.Platform {
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

// Transforms domain ExecuteRequest object into request representation
func SchedMakeThriftFromDomain(executeRequest *ExecuteRequest) *request.BazelExecuteRequest {
	if executeRequest == nil {
		return nil
	}
	return &request.BazelExecuteRequest{
		InstanceName: &executeRequest.Request.InstanceName,
		SkipCache:    &executeRequest.Request.SkipCacheLookup,
		Action:       schedMakeActionThriftFromDomain(executeRequest.Request.GetAction()),
	}
}

func schedMakeActionThriftFromDomain(action *remoteexecution.Action) *request.BazelAction {
	if action == nil {
		return nil
	}
	t := scootproto.GetMsFromDuration(action.GetTimeout())
	return &request.BazelAction{
		CommandDigest:      schedMakeDigestThriftFromDomain(action.GetCommandDigest()),
		InputDigest:        schedMakeDigestThriftFromDomain(action.GetInputRootDigest()),
		OutputFiles:        action.GetOutputFiles(),
		OutputDirs:         action.GetOutputDirectories(),
		PlatformProperties: schedMakePropertiesThriftFromDomain(action.GetPlatform()),
		TimeoutMs:          &t,
		NoCache:            &action.DoNotCache,
	}
}

func schedMakeDigestThriftFromDomain(digest *remoteexecution.Digest) *request.BazelDigest {
	if digest == nil {
		return nil
	}
	return &request.BazelDigest{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()}
}

func schedMakePropertiesThriftFromDomain(platform *remoteexecution.Platform) []*request.BazelProperty {
	if platform == nil {
		return make([]*request.BazelProperty, 0)
	}
	props := make([]*request.BazelProperty, len(platform.GetProperties()))
	for _, p := range platform.GetProperties() {
		if p == nil {
			continue
		}
		bp := &request.BazelProperty{Name: p.GetName(), Value: p.GetValue()}
		props = append(props, bp)
	}
	return props
}
