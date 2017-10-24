// +build integration

package tests

import (
	"encoding/json"
	"flag"
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
	jobBytes, err := runJob(snapshotID)
	if err != nil {
		log.Fatal(err)
	}
	jobID := string(jobBytes)

	tries := 0
	var jsonStatusBytes []byte
	status := scoot.JobStatus{}
	for tries < 10 && status.Status != scoot.Status_COMPLETED {
		jsonStatusBytes, err = getStatus(jobID)
		if err != nil {
			log.Fatal(err)
		}
		if err = json.Unmarshal(jsonStatusBytes, &status); err != nil {
			log.Fatal(err)
		}
		if status.Status == scoot.Status_COMPLETED {
			break
		}
		time.Sleep(3 * time.Second)
		tries += 1
	}
}

func installBinaries() {
	installBinary("scootapi")
	installBinary("scoot-snapshot-db")
}

func installBinary(name string) {
	cmd := exec.Command("go", "install", "./binaries/"+name)
	cmd.Run()
}

func createSnapshot() ([]byte, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		log.Fatal("GOPATH not set")
	}

	return exec.Command(gopath+"/bin/scoot-snapshot-db", "create", "ingest_dir", "--dir", ".").Output()
}

func runJob(snapshotID string) ([]byte, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		log.Fatal("GOPATH not set")
	}
	return exec.Command(gopath+"/bin/scootapi", "run_job", "sleep", "1", "--snapshot_id", snapshotID).Output()
}

func getStatus(jobID string) ([]byte, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		log.Fatal("GOPATH not set")
	}
	return exec.Command(gopath+"/bin/scootapi", "get_job_status", jobID, "--json").Output()
}
