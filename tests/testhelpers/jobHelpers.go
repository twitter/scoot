package testhelpers

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/setup"
)

// Creates a CloudScootClient that talks to the specified address
func CreateScootClient(addr string) *scootapi.CloudScootClient {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	di := dialer.NewSimpleDialer(transportFactory, protocolFactory)

	scootClient := scootapi.NewCloudScootClient(
		scootapi.CloudScootClientConfig{
			Addr:   addr,
			Dialer: di,
		})

	return scootClient
}

// Default cmd has an empty snapshot and uses sim execer on the workers.
func DefaultSnapshotCmd() *SnapshotCmd {
	return &SnapshotCmd{
		"",
		[]string{execers.UseSimExecerArg, "sleep 500", "complete 0"},
		func(s *scoot.JobStatus) error {
			if s.Status != scoot.Status_COMPLETED {
				return errors.New("Expected COMPLETED, got: " + s.String())
			} else {
				return nil
			}
		},
	}
}

// Generates a random Job and sends it to the specified client to run
// returns the JobId if successfully scheduled, otherwise "", error
func GenerateJob(numTasks int, snapshotID string) *scoot.JobDefinition {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	jobDef := GenJobDefinition(rng, numTasks, snapshotID)
	return jobDef
}

func StartJob(client *scootapi.CloudScootClient, job *scoot.JobDefinition) string {
	for {
		j, err := client.RunJob(job)
		if err == nil {
			return j.ID
		}
		// retry starting job until it succeeds.
		// this is useful for testing where we are restarting the scheduler
		log.Printf("Error Starting Job: Retrying %v", err)
	}
}

// Waits until all jobs specified have completed running or the
// specified timeout has occurred.  Periodically the status of
// running jobs is printed to the console
func WaitForJobsToCompleteAndLogStatus(
	jobIds []string,
	client scoot.CloudScoot,
	timeout time.Duration,
) error {

	jobs := make(map[string]*scoot.JobStatus)
	for _, id := range jobIds {
		jobs[id] = nil
	}

	end := time.Now().Add(timeout)
	for {
		if time.Now().After(end) {
			return fmt.Errorf("Took longer than %v", timeout)
		}
		done := true

		for jobId, oldStatus := range jobs {

			if !IsJobCompleted(oldStatus) {
				currStatus, err := client.GetStatus(jobId)

				// if there is an error just continue
				if err != nil {
					log.Printf("Error: Updating Job Status ID: %v will retry later, Error: %v", jobId, err)
					done = false
				} else {
					jobs[jobId] = currStatus
					done = done && IsJobCompleted(currStatus)
				}
			}
		}
		PrintJobs(jobs)
		if done {
			log.Println("Done")
			return nil
		}
		time.Sleep(time.Second)
	}
}

// Show job progress in the format <jobId> (<done>/<total>), e.g. ffb16fef-13fd-486c-6070-8df9c7b80dce (9997/10000)
type jobProgress struct {
	id       string
	numDone  int
	numTasks int
}

func (p jobProgress) String() string { return fmt.Sprintf("%s (%d/%d)", p.id, p.numDone, p.numTasks) }

// Prints the current status of the specified Jobs to the Log
func PrintJobs(jobs map[string]*scoot.JobStatus) {
	byStatus := make(map[scoot.Status][]string)
	for k, v := range jobs {
		st := scoot.Status_NOT_STARTED
		if v != nil {
			st = v.Status
		}
		byStatus[st] = append(byStatus[st], k)
	}

	for _, v := range byStatus {
		sort.Sort(sort.StringSlice(v))
	}

	inProgress := byStatus[scoot.Status_IN_PROGRESS]
	progs := make([]jobProgress, len(inProgress))
	for i, jobID := range inProgress {
		jobStatus := jobs[jobID]
		tasks := jobStatus.TaskStatus
		numDone := 0
		for _, st := range tasks {
			if st == scoot.Status_COMPLETED {
				numDone++
			}
		}
		progs[i] = jobProgress{id: jobID, numTasks: len(tasks), numDone: numDone}
	}

	log.Println()
	log.Println("Job Status")

	log.Println("Waiting", byStatus[scoot.Status_NOT_STARTED])
	log.Println("Running", progs)
	log.Println("Done", byStatus[scoot.Status_COMPLETED])
}

// Returns true if a job is completed or failed, false otherwise
func IsJobCompleted(s *scoot.JobStatus) bool {
	return s != nil && (s.Status == scoot.Status_COMPLETED || s.Status == scoot.Status_ROLLED_BACK)
}

// Used to match snapshot runs with expected results.
type SnapshotCmd struct {
	SnapshotID string
	Argv       []string
	Verify     func(*scoot.JobStatus) error
}

// Create cmds, for now just simple cat'ing of files in a file (non-commit) snapshot, and associated verification,
func GenerateCmds(tmp *temp.TempDir, storeAddr string, numCmds int) ([]*SnapshotCmd, error) {
	db, err := setup.NewGitDB(tmp, "", storeAddr)
	if err != nil {
		return nil, err
	}

	cmds := []*SnapshotCmd{}
	for i := 0; i < numCmds; i++ {
		dir, err := tmp.TempDir("dir")
		if err != nil {
			return nil, err
		}

		fileName := "file"
		file := filepath.Join(dir.Dir, fileName)
		content := strconv.Itoa(i)
		if err := ioutil.WriteFile(file, []byte(content), 0666); err != nil {
			return nil, err
		}

		id, err := db.IngestDir(dir.Dir)
		if err != nil {
			return nil, err
		}

		verifyFn := func(js *scoot.JobStatus) error {
			// store := bundlestore.MakeHTTPStore(bundlestore.AddrToUri(storeAddr))
			// if store, err = bundlestore.MakeCachingBrowseStore(store, tmp); err != nil {
			// 	return err
			// }

			// // All tasks should have the same exact result.
			// for _, status := range js.TaskData {
			// 	if status.Status != scoot.RunStatusState_COMPLETE {
			// 		return fmt.Errorf("RunID=%s failed: %s - %s - %d", status.RunId, status.Status, *status.Error, *status.ExitCode)
			// 	}
			// 	if reader, err := store.OpenForRead(*status.OutUri); err != nil {
			// 		return err
			// 	} else if data, err := ioutil.ReadAll(reader); err != nil {
			// 		return err
			// 	} else if string(data) != content {
			// 		return fmt.Errorf("content mismatch, expected:%s, got(%d):%s", content, len(data), string(data))
			// 	}
			// }

			return nil
		}
		cmds = append(cmds, &SnapshotCmd{string(id), []string{"cat", fileName}, verifyFn})
	}

	return cmds, nil
}
