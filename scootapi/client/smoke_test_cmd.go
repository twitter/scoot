package client

import (
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"path"
	"time"

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
	fmt.Println("Starting Smoke Test")
	fmt.Println("** Note ** Inmemory workers not supported at time since everything they do is a nop.")
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
		Tasks: map[string]*scoot.TaskDefinition{
			"id1": &scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
			},
			"id2": &scoot.TaskDefinition{
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

	out1ID := st.TaskData["id1"].SnapshotId
	out2ID := st.TaskData["id2"].SnapshotId

	log.Println("!!!!!!!!!!!!!!!!!!!!!", out1ID, out2ID)

	return nil
}

func (r *smokeTestRunner) startJob(job *scoot.JobDefinition) string {
	for {
		j, err := r.cl.scootClient.RunJob(job)
		if err == nil {
			return j.ID
		}
		// retry starting job until it succeeds.
		// this is useful for testing where we are restarting the scheduler
		log.Printf("Error Starting Job: Retrying %v", err)
	}
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
		log.Printf("Argh %v %T %+v %s", err, err, err, err.(*exec.ExitError).Stderr)
		return "", "", err
	}
	id1 = string(output)

	if err := ioutil.WriteFile(path.Join(dir.Dir, "file.txt"), []byte("second"), 0666); err != nil {
		return "", "", err
	}
	output, err = exec.Command("scoot-snapshot-db", "create", "ingest_dir", "--dir", dir.Dir).Output()
	if err != nil {
		return "", "", err
	}
	id2 = string(output)

	return id1, id2, nil
}
