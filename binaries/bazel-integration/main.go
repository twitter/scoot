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

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
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

	// Upload Command
	b, err := uploadCommand(gopath)
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	commandDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, commandDigest)
	expectedCommandDigest := remoteexecution.Digest{
		Hash:      "87d505be361979c2aa929aeeb38a61f648cc54b348f29e49ec05e0a412ad941c",
		SizeBytes: 33,
	}

	if err = assertEqual(*commandDigest, expectedCommandDigest); err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
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
	expectedRootDigest := remoteexecution.Digest{
		Hash:      bazel.EmptySha,
		SizeBytes: bazel.EmptySize,
	}
	if err = assertEqual(*rootDigest, expectedRootDigest); err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	// Upload Action
	b, err = uploadAction(gopath, bazel.DigestToStr(commandDigest), bazel.DigestToStr(rootDigest))
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	actionDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, actionDigest)
	expectedActionDigest := remoteexecution.Digest{
		Hash:      "2ea4a6b3ce4b49cb3cfd40ffacebfd2b4809fede70c7e29f3786bf6c27f8902b",
		SizeBytes: 138,
	}
	if err = assertEqual(*actionDigest, expectedActionDigest); err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	// Execute
	b, err = execute(gopath, bazel.DigestToStr(actionDigest))
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	operation := &longrunning.Operation{}
	json.Unmarshal(b, operation)
	log.Infof("Operation executing: %v", operation)
	// Get Operation
	time.Sleep(3 * time.Second)
	b, err = getOperation(gopath, operation.GetName())
	if err != nil {
		testhelpers.KillAndExit1(clusterCmds, err)
	}
	json.Unmarshal(b, operation)
	if !operation.GetDone() {
		testhelpers.KillAndExit1(clusterCmds, fmt.Errorf("Expected operation to be Done. Op: %v", operation))
	}
	clusterCmds.Kill()
}

func installBinaries() error {
	testhelpers.InstallBinary("bzutil")
	b, err := exec.Command("sh", "get_fs_util.sh").CombinedOutput()
	if err != nil {
		log.Error(string(b))
	}
	return err
}

func uploadCommand(gopath string) ([]byte, error) {
	return exec.Command(gopath+"/bin/bzutil", "upload_command", "--json", "--cas_addr=localhost:12100", "--platform_props=JDK_SYMLINK=.jvm", "sleep", "1").Output()
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
	return exec.Command(gopath+"/bin/fs_util", fmt.Sprintf("--local-store-path=%s", store.Dir), "--server-address=localhost:12100", "directory", "save", fmt.Sprintf("--root=%s", root.Dir), "**").CombinedOutput()
}

func uploadAction(gopath, commandDigest, rootDigest string) ([]byte, error) {
	return exec.Command(gopath+"/bin/bzutil", "upload_action", "--json", "--cas_addr=localhost:12100", fmt.Sprintf("--command=%s", commandDigest), fmt.Sprintf("--input_root=%s", rootDigest)).Output()
}

func execute(gopath, actionDigest string) ([]byte, error) {
	return exec.Command(gopath+"/bin/bzutil", "execute", "--json", fmt.Sprintf("--action=%s", actionDigest)).Output()
}

func getOperation(gopath, name string) ([]byte, error) {
	return exec.Command(gopath+"/bin/bzutil", "get_operation", "--json", fmt.Sprintf("--name=%s", name)).Output()
}

func assertEqual(recvd, expected remoteexecution.Digest) error {
	if recvd.Hash != expected.Hash || recvd.SizeBytes != expected.SizeBytes {
		return fmt.Errorf("Expected %v to equal received %v", expected, recvd)
	}
	return nil
}
