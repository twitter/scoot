package runners

// Bazel-related logic for runners

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/snapshot"
	bzsnapshot "github.com/twitter/scoot/snapshot/bazel"
)

// Get the Bazel Command from the embedded ExecuteRequest digest,
// and populate the runner.Command's Argv and Env fields
func fetchBazelCommand(f snapshot.Filer, cmd *runner.Command) error {
	if cmd.ExecuteRequest == nil {
		return fmt.Errorf("Command has no ExecuteRequest data")
	}
	digest := cmd.ExecuteRequest.Request.GetAction().GetCommandDigest()

	// Read command data from CAS refernced by filer
	bzFiler, ok := f.(*bzsnapshot.BzFiler)
	if !ok {
		return fmt.Errorf("Filer could not be asserted as type BzFiler")
	}
	log.Infof("Fetching Bazel Command from BzFiler server %s", bzFiler.ServerAddr)

	bzCommandBytes, err := cas.ByteStreamRead(bzFiler.ServerAddr, digest)
	if err != nil {
		log.Errorf("Error reading command data from CAS at %s: %s", bzFiler.ServerAddr, err)
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
