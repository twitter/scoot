package main

import (
	"fmt"

	s "github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"github.com/scootdev/scoot/sched/scheduler"

	"math/rand"
	"os"
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

	workCh := make(chan sched.Job)
	saga := s.MakeInMemorySaga()

	scheduler := scheduler.NewScheduler(cluster, clusterState, saga)
	scheduler.Start()

	go func() {
		generateClusterChurn(cluster, clusterState)
	}()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		generateTasks(workCh, 10000)
		wg.Done()
	}()

	for work := range workCh {
		//TODO: Error Handling
		fmt.Println("Scheduling Job: ", work.Id)
		scheduler.ScheduleJob(work)
	}

	scheduler.BlockUnitlAllJobsCompleted()

	ids, err := saga.Startup()

	// we are using an in memory saga here if we can't get the active sagas something is
	// very wrong just exit the program.
	if err != nil {
		fmt.Println("ERROR getting active sagas ", err)
		os.Exit(2)
	}

	completedSagas := 0

	for _, sagaId := range ids {

		sagaState, err := saga.RecoverSagaState(sagaId, s.ForwardRecovery)
		if err != nil {
			// For now just print error in actual scheduler we'd want to retry multiple times,
			// before putting it on a deadletter queue
			fmt.Println(fmt.Sprintf("ERROR recovering saga state for %s: %s", sagaId, err))
		}

		// all Sagas are expected to be completed
		if !sagaState.IsSagaCompleted() {
			fmt.Println(fmt.Sprintf("Expected all Sagas to be Completed %s is not", sagaId))
		} else {
			completedSagas++
		}
	}

	fmt.Println("Jobs Completed:", completedSagas)
}

/*
 * Generates work to send on the channel, using
 * Unbuffered channel because we only want to pull
 * more work when we can process it.
 *
 * For now just generates dummy tasks up to numTasks,
 * In reality this will pull off of work queue.
 */
func generateTasks(work chan<- sched.Job, numTasks int) {

	for x := 0; x < numTasks; x++ {

		work <- sched.Job{
			Id:      fmt.Sprintf("Job_%d", x),
			JobType: "testTask",
			Tasks: []sched.Task{
				sched.Task{
					Id:      fmt.Sprintf("Task_1"),
					Command: []string{"testcmd", "testcmd2"},
				},
			},
		}
	}
	close(work)
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
