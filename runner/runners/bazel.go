package runners

// Bazel-related logic for runners

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	"github.com/twitter/scoot/runner"
	bzsnapshot "github.com/twitter/scoot/snapshot/bazel"
)

// Get the Bazel Command from the embedded ExecuteRequest digest,
// and populate the runner.Command's Argv and Env fields
func fetchBazelCommand(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command) error {
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

// TODO org, naming, logs, comments, tests

func ingestFile(bzFiler *bzsnapshot.BzFiler, path string) (*remoteexecution.Digest, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Error reading file %s as bytes: %s", path, err)
	}
	sha := fmt.Sprintf("%x", sha256.Sum256(bytes))
	digest := &remoteexecution.Digest{Hash: sha, SizeBytes: int64(len(bytes))}

	casAddr, err := bzFiler.CASResolver.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Filer could not resolve a CAS server: %s", err)
	}

	err = cas.ByteStreamWrite(casAddr, digest, bytes)
	if err != nil {
		return nil, fmt.Errorf("Error writing data to CAS server %s: %s", casAddr, err)
	}

	return digest, nil
}

func ingestOutputFiles(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command, coDir string) ([]*remoteexecution.OutputFile, error) {
	outputFiles := []*remoteexecution.OutputFile{}
	for _, of := range cmd.ExecuteRequest.Request.GetAction().GetOutputFiles() {
		// join/normalize coDir and of
		// check file is there and stat executable ideally at same time
		// if it's there call bzFiler.Ingest and get digest from snapshot, also get relative path (of w/ leading slash enforced?)
		fmt.Println(of)
	}
	return outputFiles, nil
}

func ingestOutputDirs(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command, coDir string) ([]*remoteexecution.OutputDirectory, error) {
	outputDirs := []*remoteexecution.OutputDirectory{}
	return outputDirs, nil
}

func getActionResult(ofs []*remoteexecution.OutputFile,
	ods []*remoteexecution.OutputDirectory,
	rc int32,
	outD *remoteexecution.Digest,
	errD *remoteexecution.Digest) *bazelapi.ActionResult {
	return &bazelapi.ActionResult{
		Result: remoteexecution.ActionResult{
			OutputFiles:       ofs,
			OutputDirectories: ods,
			ExitCode:          rc,
			StdoutDigest:      outD,
			StderrDigest:      errD,
		},
	}
}
