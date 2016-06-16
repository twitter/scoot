package main

import (
	"fmt"

	msg "github.com/scootdev/scoot/messages"
	s "github.com/scootdev/scoot/saga"
	ci "github.com/scootdev/scoot/sched/clusterimplementations"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	distributor "github.com/scootdev/scoot/sched/distributor"

	"os"
	"sync"
)

/* demo code */
func main() {

	cluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.Members())
	fmt.Println("")

	workCh := make(chan msg.Job)
	distributor := &distributor.RoundRobin{}
	saga := s.MakeInMemorySaga()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		generateTasks(workCh, 100)
		wg.Done()
	}()

	go func() {
		scheduleWork(workCh, cluster, distributor, saga)
		wg.Done()
	}()

	wg.Wait()

	ids, err := saga.GetActiveSagas()

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

func scheduleWork(
	workCh <-chan msg.Job,
	cluster cm.Cluster,
	distributor distributor.Distributor,
	saga s.Saga) {

	var wg sync.WaitGroup
	for work := range workCh {
		node := distributor.DistributeWork(work, cluster)

		wg.Add(1)
		go func(w msg.Job, n cm.Node) {
			defer wg.Done()

			sagaId := w.Id
			state, _ := saga.StartSaga(sagaId, nil)

			//Todo: error handling, what if request fails
			for _, task := range w.Tasks {
				state, _ = saga.StartTask(state, task.Id, nil)
				n.SendMessage(task)
				state, _ = saga.EndTask(state, task.Id, nil)
			}

			state, _ = saga.EndSaga(state)
		}(work, node)

	}

	wg.Wait()
}

/*
 * Generates work to send on the channel, using
 * Unbuffered channel because we only want to pull
 * more work when we can process it.
 *
 * For now just generates dummy tasks up to numTasks,
 * In reality this will pull off of work queue.
 */
func generateTasks(work chan<- msg.Job, numTasks int) {

	for x := 0; x < numTasks; x++ {

		work <- msg.Job{
			Id:      fmt.Sprintf("Job_%d", x),
			Jobtype: "testTask",
			Tasks: []msg.Task{
				msg.Task{
					Id:       fmt.Sprintf("Task_1"),
					Commands: []string{"testcmd", "testcmd2"},
				},
			},
		}
	}
	close(work)
}
