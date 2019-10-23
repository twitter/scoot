package main

// bazel-integration creates a standalone integration testing binary.
// This mimics end-to-end action running operations from a bazel API client's perspective,
// by instantiating a cluster and using client tooling.
// Should not be run inline with other unit/property/integration tests,
// as spawned processes can cause deadlocks by e.g. colliding on known ports.

import (
	"encoding/json"
	"flag"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/genproto/googleapis/longrunning"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/scootapi/setup"
	"github.com/twitter/scoot/tests/testhelpers"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(level)

	// Initialize Local Cluster
	log.Info("Creating test cluster")
	scootClient := testhelpers.CreateScootClient(scootapi.DefaultSched_Thrift)
	clusterCmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Unexpected Error while Setting up Local Cluster %v", err))
	}
	testhelpers.WaitForClusterToBeReady(scootClient)

	log.Info("Installing binaries")
	err = installBinaries()
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}

	gopath, err := common.GetFirstGopath()
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	// TODO: WaitForClusterToBeReady should wait for CAS/ApiServers too
	time.Sleep(3 * time.Second)

	testCASOps(gopath, clusterCmds)
	testSuccessfulCommand(gopath, clusterCmds)
	testCancelledCommand(gopath, clusterCmds)

	clusterCmds.Kill()
}

func installBinaries() error {
	testhelpers.InstallBinary("bzutil")
	b, err := exec.Command("sh", "scripts/get_fs_util.sh").CombinedOutput()
	if err != nil {
		log.Error(string(b))
	}
	return err
}

func testCASOps(gopath string, clusterCmds *setup.Cmds) {
	// FindMissingBlobs

	// Upload a command and verify the digest returned is what we expect
	b, err := uploadCommand(gopath, 1)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	commandDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, commandDigest)

	expectedFound := &remoteexecution.Digest{
		Hash:      "1b00e10d51c107c1a1f06ebdc09dea3c45e06fd257481d085d4e37566f6a6041",
		SizeBytes: 76,
	}
	if !bazel.DigestsEqual(commandDigest, expectedFound) {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Unexpected digest mismatch: %s vs %s", commandDigest, expectedFound))
	}

	// Test find missing on the stored digest as well as one we expect to be missing
	expectedMissing := &remoteexecution.Digest{
		Hash:      "ed1073e458620413772fcf13e3730a6392211f04b63dde663d085eb72435d56b",
		SizeBytes: 7,
	}
	digests := bazel.DigestToStr(expectedFound) + "," + bazel.DigestToStr(expectedMissing)

	b, err = findMissingBlobs(gopath, digests)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	result := []*remoteexecution.Digest{}
	json.Unmarshal(b, &result)
	if len(result) != 1 || !bazel.DigestsEqual(result[0], expectedMissing) {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected 1 missing digest (%v). Got: %v", expectedMissing, result))
	}
}

func testSuccessfulCommand(gopath string, clusterCmds *setup.Cmds) {
	expectedCommandDigest := &remoteexecution.Digest{
		Hash:      "1b00e10d51c107c1a1f06ebdc09dea3c45e06fd257481d085d4e37566f6a6041",
		SizeBytes: 76,
	}
	expectedActionDigest := &remoteexecution.Digest{
		Hash:      "776f8cae4d90c0719121d4131ea18df38f88e20794e3907bed69195ef986a72f",
		SizeBytes: 138,
	}
	op := testRunCmd(gopath, clusterCmds, 1, expectedCommandDigest, expectedActionDigest)
	// Get Operation
	time.Sleep(3 * time.Second)
	b, err := getOperation(gopath, op.GetName())
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	op, err = execution.ExtractOpFromJson(b)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	if !op.GetDone() {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected operation to be Done. Op: %v", op))
	}
	if op.GetResponse() == nil {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected result to be set. Op: %v", op))
	}
	log.Info("Operation completed successfully")
}

func testCancelledCommand(gopath string, clusterCmds *setup.Cmds) {
	expectedCommandDigest := &remoteexecution.Digest{
		Hash:      "0392024bf028c9fd456824d64aacd8937679c451ff512c7a43da72680bf532fd",
		SizeBytes: 78,
	}
	expectedActionDigest := &remoteexecution.Digest{
		Hash:      "19ca48544f8ad500bf07b0ed06ec0fbe32f9474325d381a81a1acd23ccee52af",
		SizeBytes: 138,
	}
	op := testRunCmd(gopath, clusterCmds, 100, expectedCommandDigest, expectedActionDigest)

	// Get Operation
	b, err := getOperation(gopath, op.GetName())
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	json.Unmarshal(b, op)
	if op.GetDone() {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected operation to not be Done. Op: %v", op))
	}

	// Cancel Operation
	_, err = cancelOperation(gopath, op.GetName())
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Unable to cancel operation: %s", err))
	}

	// Get Operation and verify it was cancelled
	time.Sleep(3 * time.Second)
	b, err = getOperation(gopath, op.GetName())
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	json.Unmarshal(b, op)
	if !op.GetDone() {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected operation to be Done. Op: %v", op))
	}

	op, err = execution.ExtractOpFromJson(b)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	if op.GetError() == nil {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected result to be of type Operation_Error, was %+v", op.GetResult()))
	}
	if op.GetError().GetCode() != int32(google_rpc_code.Code_CANCELLED) {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected op.Error.Code to be %d, was %d", google_rpc_code.Code_CANCELLED, op.GetError().GetCode()))
	}
	if op.GetError().GetMessage() != "CANCELLED" {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected op.Error.Message to be 'CANCELLED', was %s", op.GetError().GetMessage()))
	}

	log.Info("Operation cancelled successfully")
}

func testRunCmd(gopath string, clusterCmds *setup.Cmds, timeToSleep int,
	expectedCommandDigest, expectedActionDigest *remoteexecution.Digest) *longrunning.Operation {
	// Upload Command
	b, err := uploadCommand(gopath, timeToSleep)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	commandDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, commandDigest)

	if !bazel.DigestsEqual(commandDigest, expectedCommandDigest) {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Unexpected digest mismatch: %s vs %s", commandDigest, expectedCommandDigest))
	}
	// Save Directory
	b, err = saveDirectory(gopath)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	s := strings.Split(strings.TrimSpace(string(b)), " ")
	sb, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	rootDigest := &remoteexecution.Digest{
		Hash:      s[0],
		SizeBytes: sb,
	}
	expectedRootDigest := &remoteexecution.Digest{
		Hash:      bazel.EmptySha,
		SizeBytes: bazel.EmptySize,
	}
	if !bazel.DigestsEqual(rootDigest, expectedRootDigest) {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Unexpected digest mismatch: %s vs %s", rootDigest, expectedRootDigest))
	}
	// Upload Action
	b, err = uploadAction(gopath, bazel.DigestToStr(commandDigest), bazel.DigestToStr(rootDigest))
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	actionDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, actionDigest)
	if !bazel.DigestsEqual(actionDigest, expectedActionDigest) {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Unexpected digest mismatch: %s vs %s", actionDigest, expectedActionDigest))
	}
	// Execute
	b, err = execute(gopath, bazel.DigestToStr(actionDigest))
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	operation := &longrunning.Operation{}
	json.Unmarshal(b, operation)
	log.Infof("Operation executing: %v", operation)
	return operation
}

// Exec'd commands

func uploadCommand(gopath string, timeToSleep int) ([]byte, error) {
	return exec.Command(
		gopath+"/bin/bzutil",
		"upload_command",
		"--json",
		"--cas_addr=localhost:12100",
		"--output_files=/output/f1",
		"--output_dirs=/output/d1,/output/subdir/d2",
		"--platform_props=JDK_SYMLINK=.jvm",
		"sleep", fmt.Sprintf("%d", timeToSleep),
	).Output()
}

func saveDirectory(gopath string) ([]byte, error) {
	root, err := temp.NewTempDir("", "root")
	if err != nil {
		return nil, err
	}
	store, err := temp.NewTempDir("", "store")
	if err != nil {
		return nil, err
	}
	return exec.Command(
		gopath+"/bin/fs_util",
		fmt.Sprintf("--local-store-path=%s", store.Dir),
		"--server-address=localhost:12100",
		"directory",
		"save",
		fmt.Sprintf("--root=%s", root.Dir), "**",
	).CombinedOutput()
}

func uploadAction(gopath, commandDigest, rootDigest string) ([]byte, error) {
	return exec.Command(
		gopath+"/bin/bzutil",
		"upload_action",
		"--json",
		"--cas_addr=localhost:12100",
		fmt.Sprintf("--command=%s", commandDigest),
		fmt.Sprintf("--input_root=%s", rootDigest),
	).Output()
}

func execute(gopath, actionDigest string) ([]byte, error) {
	return exec.Command(
		gopath+"/bin/bzutil",
		"execute",
		"--json",
		fmt.Sprintf("--action=%s", actionDigest),
	).Output()
}

func getOperation(gopath, name string) ([]byte, error) {
	return exec.Command(
		gopath+"/bin/bzutil",
		"get_operation",
		"--json",
		fmt.Sprintf("--name=%s", name),
	).Output()
}

func cancelOperation(gopath, name string) ([]byte, error) {
	return exec.Command(
		gopath+"/bin/bzutil",
		"cancel_operation",
		fmt.Sprintf("--name=%s", name),
	).Output()
}

func findMissingBlobs(gopath, digests string) ([]byte, error) {
	return exec.Command(
		gopath+"/bin/bzutil",
		"find_missing_blobs",
		"--json",
		"--cas_addr=localhost:12100",
		fmt.Sprintf("--digests=%s", digests),
	).Output()
}
