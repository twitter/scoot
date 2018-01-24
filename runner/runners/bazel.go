package runners

// Bazel-related logic for runners

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/runner"
	bzsnapshot "github.com/twitter/scoot/snapshot/bazel"
)

// Get the Bazel Command from the embedded ExecuteRequest digest,
// and populate the runner.Command's Argv and Env fields
func fetchBazelCommand(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command) error {
	if cmd.ExecuteRequest == nil {
		return fmt.Errorf("Command has no ExecuteRequest data")
	}
	digest := cmd.ExecuteRequest.Request.GetAction().GetCommandDigest()

	casAddr, err := bzFiler.CASResolver.Resolve()
	if err != nil {
		return fmt.Errorf("Filer could not resolve a CAS server: %s", err)
	}
	log.Infof("Fetching Bazel Command data from CAS server %s", casAddr)

	bzCommandBytes, err := cas.ByteStreamRead(casAddr, digest)
	if err != nil {
		log.Errorf("Error reading command data from CAS server %s: %s", casAddr, err)
		return err
	}
	bzCommand := &remoteexecution.Command{}
	if err = proto.Unmarshal(bzCommandBytes, bzCommand); err != nil {
		return fmt.Errorf("Failed to unmarshal bytes as remoteexecution.Command: %s", err)
	}

	// Update cmd's Argv and EnvVars (overwrite) from Command
	cmd.Argv = bzCommand.GetArguments()
	for _, envVar := range bzCommand.GetEnvironmentVariables() {
		cmd.EnvVars[envVar.GetName()] = envVar.GetValue()
	}

	return nil
}
