package runners

// Bazel-related logic for runners

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"time"

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
// Can return a bazelapi.ActionResult if a result was found already in the ActionCache.
// Returns slice of any digests that could not be retrieved.
func preProcessBazel(filer snapshot.Filer, cmd *runner.Command, rts *runTimes) (*bazelapi.ActionResult, []*remoteexecution.Digest, error) {
	notExist := []*remoteexecution.Digest{}

	bzFiler, ok := filer.(*bzsnapshot.BzFiler)
	if !ok {
		return nil, notExist, fmt.Errorf("Filer could not be asserted as type BzFiler. Type is: %s", reflect.TypeOf(filer))
	}
	if cmd.ExecuteRequest == nil {
		return nil, notExist, fmt.Errorf("Nil ExecuteRequest data in Command with RunType Bazel")
	}

	// Check for existing result in ActionCache
	if !cmd.ExecuteRequest.GetRequest().GetSkipCacheLookup() {
		log.Info("Checking for existing results for command in ActionCache")
		rts.actionCacheCheckStart = stamp()
		ar, err := cas.GetCacheResult(bzFiler.CASResolver, cmd.ExecuteRequest.GetRequest().GetActionDigest())
		rts.actionCacheCheckEnd = stamp()
		if err != nil {
			// Only treat as an error if we didn't get NotFoundError. We still continue:
			// cache lookup failure is internal, and should not prevent the run
			if !cas.IsNotFoundError(err) {
				log.Errorf("Failed to check for cached result, will execute: %s", err)
			}
		} else if ar != nil {
			log.Info("Returning cached result for command")
			return &bazelapi.ActionResult{
				Result:       ar,
				ActionDigest: cmd.ExecuteRequest.GetRequest().GetActionDigest(),
				Cached:       true,
			}, notExist, nil
		}
	}

	missing, err := fetchBazelCommandData(bzFiler, cmd, rts)
	if err != nil {
		log.Errorf("Error fetching Bazel command: %v", err)
		// NB: Important to return this error as-is
		// Called function can return a particular error type if the read
		// resource was not found, and we want to propagate this
		return nil, missing, err
	}
	log.Infof("Worker running with updated command arguments: %q", cmd.Argv)
	return nil, notExist, nil
}

// Get the Bazel API data from the embedded ExecuteRequest ActionDigest.
// Fetches Action, and from Action's CommandDigest, the Command.
// Overwrites runner.Command fields
func fetchBazelCommandData(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command, rts *runTimes) ([]*remoteexecution.Digest, error) {
	notExist := []*remoteexecution.Digest{}

	log.Info("Fetching Bazel Action data from CAS server")
	rts.actionFetchStart = stamp()
	actionDigest := cmd.ExecuteRequest.GetRequest().GetActionDigest()
	actionBytes, err := cas.ByteStreamRead(bzFiler.CASResolver, actionDigest)
	if err != nil {
		// NB: Important to return this error as-is
		// CAS client function returns a particular error type if the read
		// resource was not found, and we want to propagate this
		log.Errorf("Error reading Action data from CAS server: %s", err)
		notExist = append(notExist, actionDigest)
		return notExist, err
	}

	action := &remoteexecution.Action{}
	if err = proto.Unmarshal(actionBytes, action); err != nil {
		return notExist, fmt.Errorf("Failed to unmarshal bytes as remoteexecution.Action: %s", err)
	}
	rts.actionFetchEnd = stamp()

	d, err := time.ParseDuration(fmt.Sprintf("%dms", scootproto.GetMsFromDuration(action.GetTimeout())))
	if err != nil {
		return notExist, fmt.Errorf("Failed to parse Action Timeout as a Duration: %v", err)
	}

	log.Info("Fetching Bazel Command data from CAS server")
	rts.commandFetchStart = stamp()
	commandDigest := action.GetCommandDigest()
	commandBytes, err := cas.ByteStreamRead(bzFiler.CASResolver, commandDigest)
	if err != nil {
		// NB: Important to return this error as-is
		// CAS client function returns a particular error type if the read
		// resource was not found, and we want to propagate this
		log.Errorf("Error reading Command data from CAS server: %s", err)
		notExist = append(notExist, commandDigest)
		return notExist, err
	}

	bzCommand := &remoteexecution.Command{}
	if err = proto.Unmarshal(commandBytes, bzCommand); err != nil {
		return notExist, fmt.Errorf("Failed to unmarshal bytes as remoteexecution.Command: %s", err)
	}
	rts.commandFetchEnd = stamp()

	// Update cmd's SnapshotID, Argv, EnvVars, and Timeout (overwrite) from Command
	cmd.SnapshotID = bazel.SnapshotIDFromDigest(action.GetInputRootDigest())
	cmd.Argv = bzCommand.GetArguments()
	for _, envVar := range bzCommand.GetEnvironmentVariables() {
		cmd.EnvVars[envVar.GetName()] = envVar.GetValue()
	}
	cmd.Timeout = d

	// Adjust environment as specified by supported platform properties
	for _, platProp := range bzCommand.GetPlatform().GetProperties() {
		if platProp.GetName() == "JDK_SYMLINK" {
			javaHome, ok := os.LookupEnv("JAVA_HOME")
			if !ok {
				msg := "Unsuccessful lookup of $JAVA_HOME, not defined."
				log.Error(msg)
				return nil, fmt.Errorf(msg)
			}
			if b, err := exec.Command("ln", "-s", javaHome, platProp.GetValue()).CombinedOutput(); err != nil {
				log.Errorf("Error symlinking %s to %s. %s, %s", platProp.GetValue(), javaHome, string(b), err)
				return nil, err
			}
		}
	}

	// Add Action, Command to cmd's ExecuteRequest for reference
	cmd.ExecuteRequest.Action = action
	cmd.ExecuteRequest.Command = bzCommand

	return notExist, nil
}

// Post-execer actions for Bazel tasks - upload outputs and std* logs, format result structure
func postProcessBazel(filer snapshot.Filer,
	cmd *runner.Command,
	coDir string,
	stdout, stderr runner.Output,
	st execer.ProcessStatus,
	rts *runTimes) (*bazelapi.ActionResult, error) {
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
	rts.queuedTime = scootproto.GetTimeFromTimestamp(cmd.ExecuteRequest.GetExecutionMetadata().GetQueuedTimestamp())

	// Update ExecutionMetadata with invoker runTimes data and existing queued time
	// TODO Invoker should contain some metadata about the worker it lives on
	metadata := &remoteexecution.ExecutedActionMetadata{
		Worker:                         "bazel-worker",
		QueuedTimestamp:                cmd.ExecuteRequest.GetExecutionMetadata().GetQueuedTimestamp(),
		WorkerStartTimestamp:           scootproto.GetTimestampFromTime(rts.invokeStart),
		WorkerCompletedTimestamp:       scootproto.GetTimestampFromTime(rts.invokeEnd),
		InputFetchStartTimestamp:       scootproto.GetTimestampFromTime(rts.inputStart),
		InputFetchCompletedTimestamp:   scootproto.GetTimestampFromTime(rts.inputEnd),
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
	ad := cmd.ExecuteRequest.GetRequest().GetActionDigest()

	// Add result to ActionCache. Errors non-fatal
	if !cmd.ExecuteRequest.GetAction().GetDoNotCache() {
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
	for _, relPath := range cmd.ExecuteRequest.GetCommand().GetOutputFiles() {
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
	for _, relPath := range cmd.ExecuteRequest.GetCommand().GetOutputDirectories() {
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
