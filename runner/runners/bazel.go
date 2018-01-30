package runners

// Bazel-related logic for runners

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/snapshot"
	bzsnapshot "github.com/twitter/scoot/snapshot/bazel"
)

// Preliminary setup for handling Bazel commands - verify Filer and populate command
func preProcessBazel(filer snapshot.Filer, cmd *runner.Command) error {
	bzFiler, ok := filer.(*bzsnapshot.BzFiler)
	if !ok {
		return fmt.Errorf("Filer could not be asserted as type BzFiler")
	}
	if cmd.ExecuteRequest == nil {
		return fmt.Errorf("Nil ExecuteRequest data in Command with RunType Bazel")
	}
	if err := fetchBazelCommand(bzFiler, cmd); err != nil {
		return fmt.Errorf("Error retrieving Bazel command data: %v", err)
	}
	log.Infof("Worker running with updated command arguments: %q", cmd.Argv)
	return nil
}

// Get the Bazel Command from the embedded ExecuteRequest digest,
// and populate the runner.Command's Argv and Env fields
func fetchBazelCommand(bzFiler *bzsnapshot.BzFiler, cmd *runner.Command) error {
	digest := cmd.ExecuteRequest.Request.GetAction().GetCommandDigest()

	log.Info("Fetching Bazel Command data from CAS server")
	bzCommandBytes, err := cas.ByteStreamRead(bzFiler.CASResolver, digest)
	if err != nil {
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
	st execer.ProcessStatus) (*bazelapi.ActionResult, error) {
	bzFiler, ok := filer.(*bzsnapshot.BzFiler)
	if !ok {
		return nil, fmt.Errorf("Filer could not be asserted as type BzFiler")
	}
	log.Info("Processing Bazel outputs to CAS")

	stdoutDigest, err := writeFileToCAS(bzFiler, stdout.AsFile())
	if err != nil {
		return nil, fmt.Errorf("Error ingesting stdout to CAS: %s", err)
	}
	stderrDigest, err := writeFileToCAS(bzFiler, stderr.AsFile())
	if err != nil {
		return nil, fmt.Errorf("Error ingesting stderr to CAS: %s", err)
	}

	outputFiles, err := ingestOutputFiles(bzFiler, cmd, coDir)
	if err != nil {
		return nil, fmt.Errorf("Error ingesting OutputFiles: %s", err)
	}
	outputDirs, err := ingestOutputDirs(bzFiler, cmd, coDir)
	if err != nil {
		return nil, fmt.Errorf("Error ingesting OutputFiles: %s", err)
	}

	return &bazelapi.ActionResult{
		Result: remoteexecution.ActionResult{
			OutputFiles:       outputFiles,
			OutputDirectories: outputDirs,
			ExitCode:          int32(st.ExitCode),
			StdoutDigest:      stdoutDigest,
			StderrDigest:      stderrDigest,
		},
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
	for _, relPath := range cmd.ExecuteRequest.Request.GetAction().GetOutputFiles() {
		relPath = path.Join("/", relPath)
		absPath := filepath.Join(coDir, relPath)
		info, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Infof("Output file %s not present", absPath)
			} else {
				return nil, fmt.Errorf("Error Statting output file %s: %s", absPath, err)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("Expected output file %s not a regular file", absPath)
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
	for _, relPath := range cmd.ExecuteRequest.Request.GetAction().GetOutputDirectories() {
		relPath = path.Join("/", relPath)
		absPath := filepath.Join(coDir, relPath)
		info, err := os.Stat(absPath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Infof("Output dir %s not present", absPath)
			} else {
				return nil, fmt.Errorf("Error Statting output dir %s: %s", absPath, err)
			}
			continue
		}
		if !info.Mode().IsDir() {
			return nil, fmt.Errorf("Expected output dir %s not a directory", absPath)
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
