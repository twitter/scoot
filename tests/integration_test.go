// +build integration

package tests_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/tests/testhelpers"

	log "github.com/sirupsen/logrus"
)

func TestRunSimpleJob(t *testing.T) {
	log.AddHook(hooks.NewContextHook())

	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	log.Infof("Creating scoot client")
	scootClient := testhelpers.CreateScootClient(scootapi.DefaultSched_Thrift)

	// Initialize Local Cluster
	log.Infof("Creating test cluster")
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		t.Fatalf("Unexpected Error while Setting up Local Cluster %v", err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	installBinaries()

	snapshotBytes, err := createSnapshot()
	if err != nil {
		t.Fatalf("Error creating snapshotID: %v", err)
	}
	snapshotID := strings.TrimSpace(string(snapshotBytes))
	timeout := time.After(10 * time.Second)
	jobBytes, err := runJob(snapshotID)
	if err != nil {
		t.Fatal(err)
	}
	jobID := strings.TrimSpace(string(jobBytes))

	var jsonStatusBytes []byte
	status := scoot.JobStatus{}

	for status.Status != scoot.Status_COMPLETED {
		select {
		case <-timeout:
			t.Fatal("Timed out while waiting for job to complete")
		default:
			jsonStatusBytes, err = getStatus(jobID)
			if err != nil {
				t.Fatal(err)
			}
			log.Infof("jsonStatusBytes: %v", string(jsonStatusBytes))
			if err = json.Unmarshal(jsonStatusBytes, &status); err != nil {
				t.Fatal(err)
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
	return gopath, err
}

func installBinary(name string) {
	cmd := exec.Command("go", "install", "./binaries/"+name)
	cmd.Run()
}
