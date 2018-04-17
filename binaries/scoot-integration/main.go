package main

// scoot-integration creates a standalone integration testing binary.
// This mimics end-to-end job running operations from a client's perspective,
// by instantiating a cluster and using client tooling.
// Should not be run inline with other unit/property/integration tests,
// as spawned processes can cause deadlocks by e.g. colliding on known ports.

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
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

	log.Info("Creating scoot client")
	scootClient := testhelpers.CreateScootClient(scootapi.DefaultSched_Thrift)

	// Initialize Local Cluster
	log.Info("Creating test cluster")
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Fatalf("Unexpected Error while Setting up Local Cluster %v", err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	installBinaries()

	snapshotBytes, err := createSnapshot()
	if err != nil {
		log.Fatalf("Error creating snapshotID: %v", err)
	}
	snapshotID := strings.TrimSpace(string(snapshotBytes))
	timeout := time.After(10 * time.Second)
	jobBytes, err := runJob(snapshotID)
	if err != nil {
		log.Fatal(err)
	}
	jobID := strings.TrimSpace(string(jobBytes))

	var jsonStatusBytes []byte
	status := scoot.JobStatus{}

	for status.Status != scoot.Status_COMPLETED {
		select {
		case <-timeout:
			log.Fatal("Timed out while waiting for job to complete")
		default:
			jsonStatusBytes, err = getStatus(jobID)
			if err != nil {
				log.Fatal(err)
			}
			if err = json.Unmarshal(jsonStatusBytes, &status); err != nil {
				log.Fatal(err)
			}
			log.Infof("Status: %v", status)
			if status.Status == scoot.Status_COMPLETED {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func installBinaries() {
	installBinary("scootapi")
	installBinary("scoot-snapshot-db")
}

func createSnapshot() ([]byte, error) {
	gopath, err := getGopath()
	if err != nil {
		return nil, err
	}
	return exec.Command(gopath+"/bin/scoot-snapshot-db", "create", "ingest_dir", "--dir", ".").Output()
}

func runJob(snapshotID string) ([]byte, error) {
	gopath, err := getGopath()
	if err != nil {
		return nil, err
	}
	return exec.Command(gopath+"/bin/scootapi", "run_job", "sleep", "1", "--snapshot_id", snapshotID).Output()
}

func getStatus(jobID string) ([]byte, error) {
	gopath, err := getGopath()
	if err != nil {
		return nil, err
	}
	return exec.Command(gopath+"/bin/scootapi", "get_job_status", jobID, "--json").Output()
}

func getGopath() (gopath string, err error) {
	gopath = os.Getenv("GOPATH")
	if gopath == "" {
		err = fmt.Errorf("GOPATH not set")
	}
	return strings.Split(gopath, ":")[0], err
}

func installBinary(name string) {
	cmd := exec.Command("go", "install", "./binaries/"+name)
	cmd.Run()
}
