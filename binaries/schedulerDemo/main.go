package main

import (
	"fmt"

	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	api "github.com/scootdev/scoot/scootapi/server"

	"math/rand"
	"runtime"
	"sync"
	"time"
)

/* demo code */
func main() {

	runtime.GOMAXPROCS(2)

	cluster, clusterState := ci.DynamicLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.Members())
	fmt.Println("")

	sagaCoord := s.MakeInMemorySagaCoordinator()
	localSched := scheduler.NewScheduler(cluster, clusterState, sagaCoord)

	// always results in a deadlock in a long running process.
	// because we remove all nodes.  Commenting out for now
	//go func() {
	//	generateClusterChurn(cluster, clusterState)
	//}()

	var wg sync.WaitGroup
	wg.Add(1)

	workQueue := memory.NewSimpleQueue(1000)

	// generates tasks, then checks if they are all done before returning
	go func() {
		defer wg.Done()
		ids := generateTasks(workQueue, 1000)
		waitUntilJobsCompleted(ids, sagaCoord)
		fmt.Println("all tasks completed")
	}()

	//This go routine will never exit will run forever
	go func() {
		defer wg.Done()
		scheduler.GenerateWork(localSched, workQueue.Chan())
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

func generateClusterChurn(cluster cm.DynamicCluster, clusterState cm.DynamicClusterState) {

	//TODO: Make node removal more random, pick random index to remove instead
	// of always removing from end

	totalNodes := len(clusterState.InitialMembers)
	addedNodes := clusterState.InitialMembers
	removedNodes := make([]cm.Node, 0, len(addedNodes))

	for {
		// add a node
		if rand.Intn(2) != 0 {
			if len(removedNodes) > 0 {
				var n cm.Node
				n, removedNodes = removedNodes[len(removedNodes)-1], removedNodes[:len(removedNodes)-1]
				addedNodes = append(addedNodes, n)
				cluster.AddNode(n)
				fmt.Println("ADDED NODE: ", n.Id())
			} else {
				n := ci.LocalNode{
					Name: fmt.Sprintf("dynamic_node_%d", totalNodes),
				}
				totalNodes++
				addedNodes = append(addedNodes, n)
				cluster.AddNode(n)
				fmt.Println("ADDED NODE: ", n.Id())
			}
		} else {
			if len(addedNodes) > 0 {
				var n cm.Node
				n, addedNodes = addedNodes[len(addedNodes)-1], addedNodes[:len(addedNodes)-1]
				removedNodes = append(removedNodes, n)
				cluster.RemoveNode(n.Id())
				fmt.Println("REMOVED NODE: ", n.Id())
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}
