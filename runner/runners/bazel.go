package runners

// Bazel-related logic for runners

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	log "github.com/sirupsen/logrus"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	google_rpc_errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/snapshot"
	bzsnapshot "github.com/twitter/scoot/snapshot/bazel"
)

// Preliminary setup for handling Bazel commands - verify Filer and populate command.
// Can return a bazelapi.ActionResult if a result was found already in the ActionCache
func preProcessBazel(filer snapshot.Filer, cmd *runner.Command) (*bazelapi.ActionResult, error) {
	bzFiler, ok := filer.(*bzsnapshot.BzFiler)
	if !ok {
		return nil, fmt.Errorf("Filer could not be asserted as type BzFiler. Type is: %s", reflect.TypeOf(filer))
	}
	if cmd.ExecuteRequest == nil {
		return nil, fmt.Errorf("Nil ExecuteRequest data in Command with RunType Bazel")
	}

	// Check for existing result in ActionCache
	if !cmd.ExecuteRequest.GetRequest().GetSkipCacheLookup() {
		log.Info("Checking for existing results for command in ActionCache")
		ar, err := cas.GetCacheResult(bzFiler.CASResolver, cmd.ExecuteRequest.GetActionDigest())
		if err != nil {
			// Only treat as an error if we didn't get NotFoundError. We still continue:
			// cache lookup failure is internal, and should not prevent the run
			if _, ok := err.(*cas.NotFoundError); !ok {
				log.Errorf("Failed to check for cached result, will execute: %s", err)
			}
		} else if ar != nil {
			log.Info("Returning cached result for command")
			return &bazelapi.ActionResult{
				Result:       ar,
				ActionDigest: cmd.ExecuteRequest.GetActionDigest(),
				Cached:       true,
			}, nil
		}
	}

	if err := fetchBazelCommand(bzFiler, cmd); err != nil {
		log.Errorf("Error fetching Bazel command: %v", err)
		// NB: Important to return this error as-is
		// Called function can return a particular error type if the read
		// resource was not found, and we want to propagate this
		return nil, err
	}
	log.Infof("Worker running with updated command arguments: %q", cmd.Argv)
	return nil, nil
}

// Get the Bazel Command from the embedded ExecuteRequest digest,
// and populate the runner.Command's Argv and Env fields
func fetchBazelCommand(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command) error {
	digest := cmd.ExecuteRequest.GetRequest().GetAction().GetCommandDigest()

	log.Info("Fetching Bazel Command data from CAS server")
	bzCommandBytes, err := cas.ByteStreamRead(bzFiler.CASResolver, digest)
	if err != nil {
		// NB: Important to return this error as-is
		// CAS client function returns a particular error type if the read
		// resource was not found, and we want to propagate this
		log.Errorf("Error reading command data from CAS server: %s", err)
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

// Post-execer actions for Bazel tasks - upload outputs and std* logs, format result structure
func postProcessBazel(filer snapshot.Filer,
	cmd *runner.Command,
	coDir string,
	stdout, stderr runner.Output,
	st execer.ProcessStatus,
	rts runTimes) (*bazelapi.ActionResult, error) {
	bzFiler, ok := filer.(*bzsnapshot.BzFiler)
	if !ok {
		return nil, fmt.Errorf("Filer could not be asserted as type BzFiler. Type is: %s", reflect.TypeOf(filer))
	}
	log.Info("Processing Bazel outputs to CAS")

	stdoutDigest, err := writeFileToCAS(bzFiler, stdout.AsFile())
	if err != nil {
		errstr := fmt.Sprintf("Error ingesting stdout to CAS: %s", err)
		log.Error(errstr)
		return nil, fmt.Errorf(errstr)
	}
	stderrDigest, err := writeFileToCAS(bzFiler, stderr.AsFile())
	if err != nil {
		errstr := fmt.Sprintf("Error ingesting stderr to CAS: %s", err)
		log.Error(errstr)
		return nil, fmt.Errorf(errstr)
	}

	outputFiles, err := ingestOutputFiles(bzFiler, cmd, coDir)
	if err != nil {
		errstr := fmt.Sprintf("Error ingesting OutputFiles: %s", err)
		log.Error(errstr)
		return nil, fmt.Errorf(errstr)
	}
	outputDirs, err := ingestOutputDirs(bzFiler, cmd, coDir)
	if err != nil {
		errstr := fmt.Sprintf("Error ingesting OutputDirs: %s", err)
		log.Error(errstr)
		return nil, fmt.Errorf(errstr)
	}

	rts.outputEnd = stamp()
	rts.invokeEnd = stamp()

	// Update ExecutionMetadata with invoker runTimes data and existing queued time
	// TODO Invoker should contain some metadata about the worker it lives on
	metadata := &remoteexecution.ExecutedActionMetadata{
		Worker:                         "bazel-worker",
		QueuedTimestamp:                cmd.ExecuteRequest.GetExecutionMetadata().GetQueuedTimestamp(),
		WorkerStartTimestamp:           scootproto.GetTimestampFromTime(rts.invokeStart),
		WorkerCompletedTimestamp:       scootproto.GetTimestampFromTime(rts.invokeEnd),
		InputFetchStartTimestamp:       scootproto.GetTimestampFromTime(rts.checkoutStart),
		InputFetchCompletedTimestamp:   scootproto.GetTimestampFromTime(rts.checkoutEnd),
		ExecutionStartTimestamp:        scootproto.GetTimestampFromTime(rts.execStart),
		ExecutionCompletedTimestamp:    scootproto.GetTimestampFromTime(rts.execEnd),
		OutputUploadStartTimestamp:     scootproto.GetTimestampFromTime(rts.outputStart),
		OutputUploadCompletedTimestamp: scootproto.GetTimestampFromTime(rts.outputEnd),
	}

	ar := &remoteexecution.ActionResult{
		OutputFiles:       outputFiles,
		OutputDirectories: outputDirs,
		ExitCode:          int32(st.ExitCode),
		StdoutDigest:      stdoutDigest,
		StderrDigest:      stderrDigest,
		ExecutionMetadata: metadata,
	}
	ad := cmd.ExecuteRequest.GetActionDigest()

	// Add result to ActionCache. Errors non-fatal
	if !cmd.ExecuteRequest.GetRequest().GetAction().GetDoNotCache() {
		log.Info("Updating results in ActionCache")
		_, err = cas.UpdateCacheResult(bzFiler.CASResolver, ad, ar)
		if err != nil {
			log.Errorf("Error updating result to ActionCache: %s", err)
		}
	}

	return &bazelapi.ActionResult{
		Result:       ar,
		ActionDigest: ad,
		Cached:       false,
	}, nil
}

// Write a file's bytes to the BzFiler's CAS, returning the Digest of the uploaded file.
// This is distinct from using a Filer to Ingest data into the CAS,
// which allows for Filer-implementation-specific behavior that could alter the bytes and expected digest.
// (Our typical BzFiler use case is uploading Files that include relative path and other protobuf data)
func writeFileToCAS(bzFiler *bzsnapshot.BzFiler, path string) (*remoteexecution.Digest, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Error reading file %s as bytes: %s", path, err)
	}
	sha := fmt.Sprintf("%x", sha256.Sum256(bytes))
	digest := &remoteexecution.Digest{Hash: sha, SizeBytes: int64(len(bytes))}

	err = cas.ByteStreamWrite(bzFiler.CASResolver, digest, bytes)
	if err != nil {
		return nil, fmt.Errorf("Error writing data to CAS server: %s", err)
	}

	return digest, nil
}

// Ingest any of a command's OutputFiles that are specified in a Bazel ExecuteRequest.
// Files that do not exist are skipped, and this is not considered an error,
// but we do error if a specified "file" path results in a directory.
// Files located are Ingested in to the CAS via the BzFiler
func ingestOutputFiles(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command, coDir string) ([]*remoteexecution.OutputFile, error) {
	outputFiles := []*remoteexecution.OutputFile{}
	for _, relPath := range cmd.ExecuteRequest.GetRequest().GetAction().GetOutputFiles() {
		absPath := filepath.Join(coDir, relPath)
		info, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Infof("Output file %s not present, skipping ingestion", absPath)
			} else {
				return nil, fmt.Errorf("Error Statting output file %s: %s", absPath, err)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("Expected output file %s is not a regular file", absPath)
		}
		// check executable bits
		executable := (info.Mode() & 0111) > 0

		digest, err := ingestPath(bzFiler, absPath)
		if err != nil {
			return nil, err
		}
		log.Infof("Ingested OutputFile: %s", relPath)

		of := &remoteexecution.OutputFile{
			Path:         relPath,
			Digest:       digest,
			IsExecutable: executable,
		}
		outputFiles = append(outputFiles, of)
	}
	return outputFiles, nil
}

// Ingest any of a command's OutputDirectories that are specified in a Bazel ExecuteRequest.
// Directories that do not exist are skipped, and this is not considered an error,
// but we do error if a specified "directory" path results in a file.
// Directories located are Ingested in to the CAS via the BzFiler
func ingestOutputDirs(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command, coDir string) ([]*remoteexecution.OutputDirectory, error) {
	outputDirs := []*remoteexecution.OutputDirectory{}
	for _, relPath := range cmd.ExecuteRequest.GetRequest().GetAction().GetOutputDirectories() {
		absPath := filepath.Join(coDir, relPath)
		info, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Infof("Output dir %s not present, skipping ingestion", absPath)
			} else {
				return nil, fmt.Errorf("Error Statting output dir %s: %s", absPath, err)
			}
			continue
		}
		if !info.Mode().IsDir() {
			return nil, fmt.Errorf("Expected output dir %s is not a directory", absPath)
		}

		digest, err := ingestPath(bzFiler, absPath)
		if err != nil {
			return nil, err
		}
		log.Infof("Ingested OutputDirectory: %s", relPath)

		od := &remoteexecution.OutputDirectory{
			Path:       relPath,
			TreeDigest: digest,
		}
		outputDirs = append(outputDirs, od)
	}
	return outputDirs, nil
}

// Ingest a file into the BzFiler, which can store to a CAS. Take the resulting SnapshotID
// from Ingestion and return a Bazel Digest (used for direct retrieval from CAS by a client)
func ingestPath(bzFiler *bzsnapshot.BzFiler, absPath string) (*remoteexecution.Digest, error) {
	id, err := bzFiler.Ingest(absPath)
	if err != nil {
		return nil, fmt.Errorf("Error Ingesting output file %s: %s", absPath, err)
	}
	digest, err := bazel.DigestFromSnapshotID(id)
	if err != nil {
		return nil, fmt.Errorf("Error converting ingested snapshot ID %s to digest: %s", id, err)
	}
	return digest, nil
}

// Create a google RPC status "failed precondition" error with missing violation data for
// a list of non-existing Digests per Bazel API
func getFailedPreconditionStatus(notExist []*remoteexecution.Digest) (*google_rpc_status.Status, error) {
	pcf := &google_rpc_errdetails.PreconditionFailure{
		Violations: []*google_rpc_errdetails.PreconditionFailure_Violation{},
	}

	for _, ne := range notExist {
		pcf.Violations = append(pcf.Violations, &google_rpc_errdetails.PreconditionFailure_Violation{
			Type:    bazelapi.PreconditionMissing,
			Subject: fmt.Sprintf("blobs/%s/%d", ne.GetHash(), ne.GetSizeBytes()),
		})
	}

	pcfAsAny, err := ptypes.MarshalAny(pcf)
	if err != nil {
		return nil, fmt.Errorf("Failed to serialize PreconditionFailure data: %s", err)
	}

	s := &google_rpc_status.Status{
		Code:    int32(google_rpc_code.Code_FAILED_PRECONDITION),
		Details: []*any.Any{pcfAsAny},
	}
	return s, nil
}

// Wrapper for returing precondition failure status from missing checkout/input root digest
func getCheckoutMissingStatus(snapshotID string) (*google_rpc_status.Status, error) {
	inputRoot, err := bazel.DigestFromSnapshotID(snapshotID)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert SnapshotID %s to Digest: %s", snapshotID, err)
	}
	return getFailedPreconditionStatus([]*remoteexecution.Digest{inputRoot})
}
