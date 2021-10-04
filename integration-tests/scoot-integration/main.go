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
	"os/exec"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/scheduler"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/tests/testhelpers"
	"github.com/twitter/scoot/worker/domain"
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

	log.Info("Creating scoot client")
	scootClient := testhelpers.CreateScootClient(scheduler.DefaultSched_Thrift)

	// Initialize Local Cluster
	log.Info("Creating test cluster")
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	testhelpers.InstallBinaries()
	gopath, err := common.GetFirstGopath()
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	snapshotBytes, err := createSnapshot(gopath)
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	snapshotID := strings.TrimSpace(string(snapshotBytes))
	timeout := time.After(10 * time.Second)
	jobBytes, err := runJob(gopath, snapshotID)
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	jobID := strings.TrimSpace(string(jobBytes))

	var jsonStatusBytes []byte
	status := scoot.JobStatus{}
	for status.Status != scoot.Status_COMPLETED {
		select {
		case <-timeout:
			testhelpers.KillAndExit1(cluster1Cmds, fmt.Errorf("Timed out while waiting for job to complete"))
		default:
			jsonStatusBytes, err = getStatus(gopath, jobID)
			if err != nil {
				testhelpers.KillAndExit1(cluster1Cmds, err)
			}
			if err = json.Unmarshal(jsonStatusBytes, &status); err != nil {
				testhelpers.KillAndExit1(cluster1Cmds, err)
			}
			log.Infof("Status: %v", status)
			if status.Status == scoot.Status_COMPLETED {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	_, err = offlineWorker(gopath, fmt.Sprintf("localhost:%d", domain.WorkerPorts+1))
	if err != nil {
		log.Error(err)
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	jobIDs, err := runXJobs(gopath, snapshotID, 10)
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	jobIDToStatus := make(map[string]scoot.JobStatus)
	for _, jobID := range jobIDs {
		jobIDToStatus[jobID] = scoot.JobStatus{}
	}
	for jobID, status := range jobIDToStatus {
		timeout = time.After(10 * time.Second)
		for status.Status != scoot.Status_COMPLETED {
			select {
			case <-timeout:
				testhelpers.KillAndExit1(cluster1Cmds, fmt.Errorf("Timed out while waiting for job to complete"))
			default:
				jsonStatusBytes, err = getStatus(gopath, jobID)
				if err != nil {
					testhelpers.KillAndExit1(cluster1Cmds, err)
				}
				if err = json.Unmarshal(jsonStatusBytes, &status); err != nil {
					testhelpers.KillAndExit1(cluster1Cmds, err)
				}
				log.Infof("Status: %v", status)
				if status.Status == scoot.Status_COMPLETED {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}
	}

	_, err = reinstateWorker(gopath, fmt.Sprintf("localhost:%d", domain.WorkerPorts+1))
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	jobIDs, err = runXJobs(gopath, snapshotID, 10)
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	jobIDToStatus = make(map[string]scoot.JobStatus)
	for _, jobID := range jobIDs {
		jobIDToStatus[jobID] = scoot.JobStatus{}
	}
	for jobID, status := range jobIDToStatus {
		timeout = time.After(10 * time.Second)
		for status.Status != scoot.Status_COMPLETED {
			select {
			case <-timeout:
				testhelpers.KillAndExit1(cluster1Cmds, fmt.Errorf("Timed out while waiting for job to complete"))
			default:
				jsonStatusBytes, err = getStatus(gopath, jobID)
				if err != nil {
					testhelpers.KillAndExit1(cluster1Cmds, err)
				}
				if err = json.Unmarshal(jsonStatusBytes, &status); err != nil {
					testhelpers.KillAndExit1(cluster1Cmds, err)
				}
				log.Infof("Status: %v", status)
				if status.Status == scoot.Status_COMPLETED {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}
	}

	cluster1Cmds.Kill()
}

func createSnapshot(gopath string) ([]byte, error) {
	return exec.Command(gopath+"/bin/scoot-snapshot-db", "create", "ingest_dir", "--dir", ".").Output()
}

func runJob(gopath, snapshotID string) ([]byte, error) {
	return exec.Command(gopath+"/bin/scootcl", "run_job", "sleep", "1", "--snapshot_id", snapshotID).Output()
}

func runXJobs(gopath, snapshotID string, x int) ([]string, error) {
	jobIDs := []string{}
	for i := 0; i < x; i++ {
		jobBytes, err := runJob(gopath, snapshotID)
		if err != nil {
			return nil, err
		}
		jobID := strings.TrimSpace(string(jobBytes))
		jobIDs = append(jobIDs, jobID)
	}
	return jobIDs, nil
}

func getStatus(gopath, jobID string) ([]byte, error) {
	return exec.Command(gopath+"/bin/scootcl", "get_job_status", jobID, "--json").Output()
}

func offlineWorker(gopath, workerID string) ([]byte, error) {
	return exec.Command(gopath+"/bin/scootcl", "offline_worker", workerID).Output()
}

func reinstateWorker(gopath, workerID string) ([]byte, error) {
	return exec.Command(gopath+"/bin/scootcl", "reinstate_worker", workerID).Output()
}
