// Execution Request Definitions
// Domain structures for tracking ExecuteRequest info (use cases include Scheduler JobDefs and
// Worker RunCommands), and conversions for internal thrift APIs.
package bazel

import (
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	bazelthrift "github.com/twitter/scoot/bazel/execution/gen-go/bazel"
	scootproto "github.com/twitter/scoot/common/proto"
)

// This type gives us a single reference point for passing Execute Requests and
// leaves room for internal modifications if required
type ExecuteRequest struct {
	Request remoteexecution.ExecuteRequest
}

func (e *ExecuteRequest) String() string {
	if e == nil {
		return ""
	}
	return e.Request.String()
}

// Transform request Bazel ExecuteRequest data into a domain object
func MakeDomainFromThrift(thriftRequest *bazelthrift.ExecuteRequest) *ExecuteRequest {
	if thriftRequest == nil {
		return nil
	}
	er := remoteexecution.ExecuteRequest{}
	er.InstanceName = thriftRequest.GetInstanceName()
	er.SkipCacheLookup = thriftRequest.GetSkipCache()
	er.Action = makeActionFromThrift(thriftRequest.GetAction())
	return &ExecuteRequest{Request: er}
}

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

// Transforms domain ExecuteRequest object into request representation
func MakeThriftFromDomain(executeRequest *ExecuteRequest) *bazelthrift.ExecuteRequest {
	if executeRequest == nil {
		return nil
	}
	return &bazelthrift.ExecuteRequest{
		InstanceName: &executeRequest.Request.InstanceName,
		SkipCache:    &executeRequest.Request.SkipCacheLookup,
		Action:       makeActionThriftFromDomain(executeRequest.Request.GetAction()),
	}
}

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
