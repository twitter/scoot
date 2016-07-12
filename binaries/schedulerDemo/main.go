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
	sagaCoord := s.MakeInMemorySagaCoordinator()

	scheduler := scheduler.NewScheduler(cluster, clusterState, sagaCoord)

	go func() {
		generateClusterChurn(cluster, clusterState)
	}()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		generateTasks(workCh, 100000)
		wg.Done()
	}()

	for work := range workCh {
		//TODO: Error Handling
		fmt.Println("Scheduling Job: ", work.Id)
		scheduler.ScheduleJob(work)
	}

	scheduler.BlockUnitlAllJobsCompleted()

	ids, err := sagaCoord.Startup()

	// we are using an in memory saga here if we can't get the active sagas something is
	// very wrong just exit the program.
	if err != nil {
		fmt.Println("ERROR getting active sagas ", err)
		os.Exit(2)
	}

	completedSagas := 0

	for _, sagaId := range ids {

		saga, err := sagaCoord.RecoverSagaState(sagaId, s.ForwardRecovery)
		if err != nil {
			// For now just print error in actual scheduler we'd want to retry multiple times,
			// before putting it on a deadletter queue
			fmt.Println(fmt.Sprintf("ERROR recovering saga state for %s: %s", sagaId, err))
		}

		// all Sagas are expected to be completed
		if !saga.GetState().IsSagaCompleted() {
			fmt.Println(fmt.Sprintf("Expected all Sagas to be Completed %s is not", sagaId))
		} else {
			completedSagas++
		}
	}

	fmt.Println("Jobs Completed:", completedSagas)
}

// <<<<<<< fd5b67e8f41c7804994527e660a4d6de69d84e09:binaries/schedulerDemo/main.go
// =======
// func scheduleWork(
// 	workCh <-chan sched.Job,
// 	distributor *distributor.PoolDistributor,
// 	saga s.Saga) {

// 	var wg sync.WaitGroup
// 	for work := range workCh {
// 		node := distributor.ReserveNode(work)

// 		wg.Add(1)
// 		go func(w sched.Job, n cm.Node) {
// 			defer wg.Done()

// 			sagaId := w.Id
// 			state, _ := saga.StartSaga(sagaId, nil)

// 			//Todo: error handling, what if request fails
// 			for id, task := range w.Def.Tasks {
// 				state, _ = saga.StartTask(state, id, nil)
// 				n.SendMessage(task)
// 				state, _ = saga.EndTask(state, id, nil)
// 			}

// 			state, _ = saga.EndSaga(state)
// 			distributor.ReleaseNode(n)
// 		}(work, node)

// 	}

// 	wg.Wait()
// }

// >>>>>>> scootapi: refactor and rename Thrift:sched/demo/main.go
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
			Id: fmt.Sprintf("Job_%d", x),
			Def: sched.JobDefinition{
				JobType: "testTask",
				Tasks: map[string]sched.TaskDefinition{
					"Task_1": sched.TaskDefinition{

						Command: sched.Command{
							Argv: []string{"testcmd", "testcmd2"},
						},
					},
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
