package main

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
		log.Error(err)
		return
	}
	log.SetLevel(level)

	// Initialize Local Cluster
	log.Info("Creating test cluster")
	scootClient := testhelpers.CreateScootClient(scootapi.DefaultSched_Thrift)
	clusterCmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Errorf("Unexpected Error while Setting up Local Cluster %v", err)
		return
	}
	defer clusterCmds.Kill()
	testhelpers.WaitForClusterToBeReady(scootClient)

	log.Info("Installing binaries")
	err = installBinaries()
	if err != nil {
		log.Error(err)
		return
	}

	gopath, err := testhelpers.GetGopath()
	if err != nil {
		log.Error(err)
		return
	}
	time.Sleep(5 * time.Second)
	// Upload Command
	b, err := uploadCommand(gopath)
	if err != nil {
		log.Error(err)
		return
	}
	commandDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, commandDigest)
	expectedCommandDigest := remoteexecution.Digest{
		Hash:      "a9634eafd1d962a7949905b01b50176c3678cd5e5ed35db5519f7c636bd82285",
		SizeBytes: 10,
	}
	if err = assertEqual(*commandDigest, expectedCommandDigest); err != nil {
		log.Error(err)
		return
	}
	// Save Directory
	b, err = saveDirectory(gopath)
	log.Info(string(b))
	if err != nil {
		log.Error(err)
		return
	}
	s := strings.Split(strings.TrimSpace(string(b)), " ")
	sb, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		log.Error(err)
		return
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
		log.Error(err)
		return
	}
	// Upload Action
	b, err = uploadAction(gopath, bazel.DigestToStr(commandDigest), bazel.DigestToStr(rootDigest))
	if err != nil {
		log.Error(err)
		return
	}
	actionDigest := &remoteexecution.Digest{}
	json.Unmarshal(b, actionDigest)
	expectedActionDigest := remoteexecution.Digest{
		Hash:      "47f24e7e3920ea2516271280e767d66db707096e6fa552a7ad008900bdd8291d",
		SizeBytes: int64(138),
	}
	if err = assertEqual(*actionDigest, expectedActionDigest); err != nil {
		log.Error(err)
		return
	}
	// Execute
	b, err = execute(gopath, bazel.DigestToStr(actionDigest))
	if err != nil {
		log.Error(err)
		return
	}
	operation := &longrunning.Operation{}
	json.Unmarshal(b, operation)
	log.Infof("Operation executing: %v", operation)
	// Get Operation
	time.Sleep(5 * time.Second)
	b, err = getOperation(gopath, operation.GetName())
	json.Unmarshal(b, operation)
	if !operation.GetDone() {
		log.Error("Expected operation to be Done. Op: %v", operation)
		return
	}
}

func installBinaries() error {
	testhelpers.InstallBinary("bzutil")
	b, err := exec.Command("sh", "get_fs_util.sh").CombinedOutput()
	if err != nil {
		log.Errorf("Error fetching fs_util. %s: %s", err, string(b))
		return err
	}
	return nil
}

func uploadCommand(gopath string) ([]byte, error) {
	return exec.Command(gopath+"/bin/bzutil", "upload_command", "--json", "--cas_addr=localhost:12100", "sleep", "3").Output()
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
	return exec.Command(gopath+"/bin/fs_util", fmt.Sprintf("--local-store-path=%s", store.Dir), "--server-address=localhost:12100", "directory", "save", "--root", root.Dir, "**").CombinedOutput()
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
