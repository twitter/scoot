package main

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
	ci "github.com/scootdev/scoot/cloud/cluster/memory"
	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/distributor"
	"github.com/scootdev/scoot/sched/queue"
	qi "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker/fake"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	api "github.com/scootdev/scoot/scootapi/server"
)

/* demo code */
func main() {

	runtime.GOMAXPROCS(2)

	updateCh := make(chan []cluster.NodeUpdate)
	cluster := ci.NewCluster(ci.NewIdNodes(10), updateCh)
	dist, err := distributor.NewPoolDistributorFromCluster(cluster)
	if err != nil {
		log.Fatalf("Could not create distributor: %v", err)
	}

	members := cluster.Members()
	fmt.Println("clusterMembers:", members)
	fmt.Println("")

	sagaCoord := s.MakeInMemorySagaCoordinator()
	sched := scheduler.NewScheduler(dist, sagaCoord, fake.MakeWaitingNoopWorker)

	// always results in a deadlock in a long running process.
	// because we remove all nodes.  Commenting out for now
	//go func() {
	//	generateClusterChurn(cluster, clusterState)
	//}()

	var wg sync.WaitGroup
	wg.Add(1)

	workQueue := qi.NewSimpleQueue(1000)

	// generates tasks, then checks if they are all done before returning
	go func() {
		defer wg.Done()
		ids := generateTasks(workQueue, 1000)
		waitUntilJobsCompleted(ids, sagaCoord)
		workQueue.Close()
	}()

	go func() {
		defer wg.Done()
		scheduler.GenerateWork(sched, workQueue.Chan())
	}()

	wg.Wait()
}

// Checks the State of all Passed in jobs.  When they are all
// completed returns
func waitUntilJobsCompleted(ids []string, sc s.SagaCoordinator) {

	completedJobs := 0

	for completedJobs < len(ids) {
		time.Sleep(100 * time.Millisecond)
		completedJobs = 0
		for _, id := range ids {
			status, _ := api.GetJobStatus(id, sc)
			if status.Status == scoot.Status_COMPLETED {
				completedJobs++
			}
		}
	}
}

/*
 * Generates work enqueus to a queue, returns a list of job ids
 */
func generateTasks(workQueue queue.Queue, numTasks int) []string {
	ids := make([]string, 0, numTasks)

	for x := 0; x < numTasks; x++ {
		jobDef := sched.JobDefinition{
			JobType: "testTask",
			Tasks: map[string]sched.TaskDefinition{
				"Task_1": sched.TaskDefinition{
					Command: sched.Command{
						Argv: []string{"testcmd", "testcmd2"},
					},
				},
			},
		}

		id, _ := workQueue.Enqueue(jobDef)
		ids = append(ids, id)
	}

	return ids
}

func generateClusterChurn(cl cluster.Cluster, updateCh chan []cluster.NodeUpdate) {

	//TODO: Make node removal more random, pick random index to remove instead
	// of always removing from end

	i := 0
	removed := make([]cluster.NodeId, 0)

	for {
		if rand.Intn(2) != 0 {
			// add a node
			var id string

			if len(removed) > 0 {
				// Recycle
				id = string(removed[0])
				removed = removed[1:]
			} else {
				id = fmt.Sprintf("new_node_%d", i)
				i++
			}
			updateCh <- []cluster.NodeUpdate{cluster.NewAdd(ci.NewIdNode(id))}
			fmt.Println("ADDED NODE: ", id)
		} else {
			members := cl.Members()
			if len(members) == 0 {
				continue
			}
			which := rand.Intn(len(members))
			id := members[which].Id()
			removed = append(removed, id)
			updateCh <- []cluster.NodeUpdate{cluster.NewRemove(id)}
			fmt.Println("REMOVED NODE: ", id)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
