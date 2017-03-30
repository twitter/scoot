package client

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"strings"
	"time"

	log "github.com/inconshreveable/log15"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/tests/testhelpers"
	"github.com/spf13/cobra"
)

type smokeTestCmd struct {
	numJobs   int
	numTasks  int
	timeout   time.Duration
	storeAddr string
}

func (c *smokeTestCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_smoke_test",
		Short: "Smoke Test",
	}
	r.Flags().IntVar(&c.numJobs, "num_jobs", 100, "number of jobs to run")
	r.Flags().IntVar(&c.numTasks, "num_tasks", -1, "number of tasks per job, or random if -1")
	r.Flags().DurationVar(&c.timeout, "timeout", 180*time.Second, "how long to wait for the smoke test")
	r.Flags().StringVar(&c.storeAddr, "bundlestore", "", "address in the form of host:port")

	return r
}

func (c *smokeTestCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	tmp, err := temp.TempDirDefault()
	if err != nil {
		return err
	}
	log.Info("Starting Smoke Test")
	log.Info("** Note ** Inmemory workers not supported at time since everything they do is a nop.")
	runner := &smokeTestRunner{cl: cl, tmp: tmp}
	if err := runner.run(c.numJobs, c.numTasks, c.timeout); err != nil {
		panic(err) // returning err would make cobra print out usage, which doesn't make sense to do here.
	}
	return nil
}

type smokeTestRunner struct {
	cl  *simpleCLIClient
	tmp *temp.TempDir
}

func (r *smokeTestRunner) run(numJobs int, numTasks int, timeout time.Duration) error {
	id1, id2, err := r.generateSnapshots()
	if err != nil {
		return err
	}
	// Generate the jobs and start executing.
	jobs := make([]string, numJobs)

	// (first job will test data)
	jobs[0] = testhelpers.StartJob(r.cl.scootClient, &scoot.JobDefinition{
		// Repeat tasks to better exercise Store w/groupcache.
		Tasks: map[string]*scoot.TaskDefinition{
			"id1": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
			},
			"id2": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
			},
			"id3": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
			},
			"id4": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
			},
			"id5": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
			},
			"id6": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
			},
		}})

	for i := 1; i < numJobs; i++ {
		jobs[i] = testhelpers.StartJob(r.cl.scootClient, testhelpers.GenerateJob(numTasks, id1))
	}

	// Wait for results and then verify that the results are as expected.
	if err := testhelpers.WaitForJobsToCompleteAndLogStatus(jobs, r.cl.scootClient, timeout); err != nil {
		return err
	}

	st, err := r.cl.scootClient.GetStatus(jobs[0])
	if err != nil {
		return err
	}

	out1ID := *st.TaskData["id1"].SnapshotId
	out2ID := *st.TaskData["id2"].SnapshotId

	return r.checkSnapshots(out1ID, out2ID)
}

func (r *smokeTestRunner) generateSnapshots() (id1 string, id2 string, err error) {
	dir, err := r.tmp.TempDir("testdata")
	if err != nil {
		return "", "", err
	}

	if err := ioutil.WriteFile(path.Join(dir.Dir, "file.txt"), []byte("first"), 0666); err != nil {
		return "", "", err
	}
	output, err := exec.Command("scoot-snapshot-db", "create", "ingest_dir", "--dir", dir.Dir).Output()
	if err != nil {
		return "", "", err
	}
	id1 = strings.TrimSuffix(string(output), "\n")

	if err := ioutil.WriteFile(path.Join(dir.Dir, "file.txt"), []byte("second"), 0666); err != nil {
		return "", "", err
	}
	output, err = exec.Command("scoot-snapshot-db", "create", "ingest_dir", "--dir", dir.Dir).Output()
	if err != nil {
		return "", "", err
	}
	id2 = strings.TrimSuffix(string(output), "\n")

	return id1, id2, nil
}

func (r *smokeTestRunner) checkSnapshots(id1 string, id2 string) error {
	output, err := exec.Command("scoot-snapshot-db", "read", "cat", "--id", id1, "STDOUT").Output()
	if err != nil {
		return err
	}
	text := string(output)
	if text != "first" {
		return fmt.Errorf("expected first out snapshot %v to contain \"first\" but got %q", id1, text)
	}

	output, err = exec.Command("scoot-snapshot-db", "read", "cat", "--id", id2, "STDOUT").Output()
	if err != nil {
		return err
	}
	text = string(output)
	if text != "second" {
		return fmt.Errorf("expected second out snapshot %v to contain \"second\" but got %q", id2, text)
	}

	return nil
}
