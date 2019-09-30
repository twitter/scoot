package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/os/temp"
	"github.com/twitter/scoot/common/tests/testhelpers"
	"github.com/twitter/scoot/scheduler/scootapi/client"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	// httpAddr := flag.String("addr", "", "'host:port' addr to serve http on")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	// storeAddr := flag.String("bundlestore", "", "address in the form of host:port")
	numJobs := flag.Int("num_jobs", 100, "number of jobs to run")
	numTasks := flag.Int("num_tasks", -1, "number of tasks per job, or random if -1")
	timeout := flag.Duration("timeout", 180*time.Second, "how long to wait for the smoke test")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	scootClient := testhelpers.CreateScootClient(client.DefaultSched_Thrift)
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	tmp, err := temp.TempDirDefault()
	if err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}
	log.Info("Starting Smoke Test")
	log.Info("** Note ** Inmemory workers not supported at time since everything they do is a nop.")
	runner := &smokeTestRunner{cl: scootClient, tmp: tmp}
	if err := runner.run(*numJobs, *numTasks, *timeout); err != nil {
		testhelpers.KillAndExit1(cluster1Cmds, err)
	}

}

type smokeTestRunner struct {
	cl  *client.CloudScootClient
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
	t1, t2, t3, t4, t5, t6 := "id1", "id2", "id3", "id4", "id5", "id6"
	jobs[0] = testhelpers.StartJob(r.cl, &scoot.JobDefinition{
		// Repeat tasks to better exercise Store w/groupcache.
		Tasks: []*scoot.TaskDefinition{
			&scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
				TaskId:     &t1,
			},
			&scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
				TaskId:     &t2,
			},
			&scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
				TaskId:     &t3,
			},
			&scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id1,
				TaskId:     &t4,
			},
			&scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
				TaskId:     &t5,
			},
			&scoot.TaskDefinition{
				Command:    &scoot.Command{Argv: []string{"cat", "file.txt"}},
				SnapshotId: &id2,
				TaskId:     &t6,
			},
		}})

	for i := 1; i < numJobs; i++ {
		jobs[i] = testhelpers.StartJob(r.cl, testhelpers.GenerateJob(numTasks, id1))
	}

	// Wait for results and then verify that the results are as expected.
	if err := testhelpers.WaitForJobsToCompleteAndLogStatus(jobs, r.cl, timeout); err != nil {
		return err
	}

	st, err := r.cl.GetStatus(jobs[0])
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
	// Note: worker output had a header that ends with "SCOOT_CMD_LOG". Just check the end of string and ignore the rest.
	output, err := exec.Command("scoot-snapshot-db", "read", "cat", "--id", id1, "STDOUT").Output()
	if err != nil {
		return err
	}
	text := string(output)
	if ok, _ := regexp.MatchString(`(?s).*first\z`, text); !ok {
		return fmt.Errorf("expected first out snapshot %v to contain \"first\" but got %q", id1, text)
	}

	output, err = exec.Command("scoot-snapshot-db", "read", "cat", "--id", id2, "STDOUT").Output()
	if err != nil {
		return err
	}
	text = string(output)
	if ok, _ := regexp.MatchString(`(?s).*second\z`, text); !ok {
		return fmt.Errorf("expected second out snapshot %v to contain \"second\" but got %q", id2, text)
	}

	return nil
}
