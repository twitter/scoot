package smoketest

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/common/client"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/tests/testhelpers"
)

type SmokeTestCmd struct {
	numJobs   int
	numTasks  int
	timeout   time.Duration
	storeAddr string
}

func (c *SmokeTestCmd) RegisterFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "smoketest",
		Short: "SmokeTest",
	}
	r.Flags().IntVar(&c.numJobs, "num_jobs", 100, "number of jobs to run")
	r.Flags().IntVar(&c.numTasks, "num_tasks", -1, "number of tasks per job, or random if -1")
	r.Flags().DurationVar(&c.timeout, "timeout", 180*time.Second, "how long to wait for the smoke test")
	r.Flags().StringVar(&c.storeAddr, "bundlestore", "", "address in the form of host:port")

	return r
}

func (c *SmokeTestCmd) Run(cl *client.SimpleClient, cmd *cobra.Command, args []string) error {
	tmp, err := ioutil.TempDir("", "")
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
	cl  *client.SimpleClient
	tmp string
}

func (r *smokeTestRunner) run(numJobs int, numTasks int, timeout time.Duration) error {
	id1, id2, err := r.generateSnapshots()
	if err != nil {
		return err
	}
	// Generate the jobs and start executing.
	jobs := make([]string, numJobs)

	// (first job will test data)
	t1, t2, t3, t4, t5, t6 := "id1", "id2", "id3", "id4", "id5", "id6"
	jobs[0] = testhelpers.StartJob(r.cl.ScootClient, &scoot.JobDefinition{
		// Repeat tasks to better exercise Store w/groupcache.
		Tasks: []*scoot.TaskDefinition{
			{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
				TaskId:     &t1,
			},
			{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
				TaskId:     &t2,
			},
			{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
				TaskId:     &t3,
			},
			{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
				TaskId:     &t4,
			},
			{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
				TaskId:     &t5,
			},
			{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
				TaskId:     &t6,
			},
		}})

	for i := 1; i < numJobs; i++ {
		jobs[i] = testhelpers.StartJob(r.cl.ScootClient, testhelpers.GenerateJob(numTasks, id1))
	}

	// Wait for results and then verify that the results are as expected.
	if err := testhelpers.WaitForJobsToCompleteAndLogStatus(jobs, r.cl.ScootClient, timeout); err != nil {
		return err
	}

	st, err := r.cl.ScootClient.GetStatus(jobs[0])
	if err != nil {
		return err
	}

	outUri1 := *st.TaskData["id1"].OutUri
	outUri2 := *st.TaskData["id2"].OutUri
	return r.checkOutputs(outUri1, outUri2)
}

func (r *smokeTestRunner) generateSnapshots() (id1 string, id2 string, err error) {
	dir, err := ioutil.TempDir(r.tmp, "testdata")
	if err != nil {
		return "", "", err
	}

	if err := ioutil.WriteFile(path.Join(dir, "file.txt"), []byte("first"), 0666); err != nil {
		return "", "", err
	}
	output, err := exec.Command("scoot-snapshot-db", "create", "ingest_dir", "--dir", dir).Output()
	if err != nil {
		return "", "", err
	}
	id1 = strings.TrimSuffix(string(output), "\n")

	if err := ioutil.WriteFile(path.Join(dir, "file.txt"), []byte("second"), 0666); err != nil {
		return "", "", err
	}
	output, err = exec.Command("scoot-snapshot-db", "create", "ingest_dir", "--dir", dir).Output()
	if err != nil {
		return "", "", err
	}
	id2 = strings.TrimSuffix(string(output), "\n")

	return id1, id2, nil
}

// Note: worker output had a header that ends with "SCOOT_CMD_LOG". Just check the end of string and ignore the rest.
func (r *smokeTestRunner) checkOutputs(outUri1 string, outUri2 string) error {
	// get the absolute path of stdout file
	re, _ := regexp.Compile("file://.*?(/.*)")
	outFile := re.FindStringSubmatch(outUri1)[1]
	body, err := ioutil.ReadFile(outFile)
	if err != nil {
		return fmt.Errorf("unable to read file: %v", err)
	}
	text := string(body)
	if ok, _ := regexp.MatchString(`(?s).*first\z`, text); !ok {
		return fmt.Errorf("expected first output file %v to contain \"first\" but got %q", outFile, text)
	}

	outFile = re.FindStringSubmatch(outUri2)[1]
	body, err = ioutil.ReadFile(outFile)
	if err != nil {
		return fmt.Errorf("unable to read file: %v", err)
	}
	text = string(body)
	if ok, _ := regexp.MatchString(`(?s).*second\z`, text); !ok {
		return fmt.Errorf("expected second output file %v to contain \"second\" but got %q", outFile, text)
	}
	return nil
}
