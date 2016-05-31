package main

import "fmt"
import "sync"

import msg "github.com/scootdev/scoot/messages"
import saga "github.com/scootdev/scoot/saga"
import ci "github.com/scootdev/scoot/sched/clusterimplementations"
import cm "github.com/scootdev/scoot/sched/clustermembership"
import distributor "github.com/scootdev/scoot/sched/distributor"

/* demo code */
func main() {

	cluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.Members())
	fmt.Println("")

	workCh := make(chan msg.Job)
	distributor := &distributor.RoundRobin{}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		generateTasks(workCh, 100)
		wg.Done()
	}()

	go func() {
		scheduleWork(workCh, cluster, distributor)
		wg.Done()
	}()

	wg.Wait()
}

func scheduleWork(
	workCh <-chan msg.Job,
	cluster cm.Cluster,
	distributor distributor.Distributor) {

	saga := saga.InMemorySagaFactory()
	workIds := make([]string, 0)

	var wg sync.WaitGroup
	for work := range workCh {
		node := distributor.DistributeWork(work, cluster)
		workIds = append(workIds, work.Id)

		wg.Add(1)
		go func(w msg.Job, n cm.Node) {
			sagaId := w.Id
			saga.StartSaga(sagaId, nil)

			defer wg.Done()
			//Todo: error handling, what if request fails
			for _, task := range w.Tasks {
				saga.StartTask(sagaId, task.Id)
				n.SendMessage(task)
				saga.EndTask(sagaId, task.Id, nil)
			}

			saga.EndSaga(sagaId)
		}(work, node)

	}

	wg.Wait()

	//Verify all work completed
	errs := 0
	for _, id := range workIds {
		state, err := saga.GetSagaState(id)
		if err != nil {
			fmt.Println("Error Processing Saga: ", id, "never scheduled")
			errs++
		}

		if !state.IsSagaCompleted() {
			fmt.Println("Error Processing Saga: ", id, "schedulded but never finished")
			errs++
		}
	}

	if errs == 0 {
		fmt.Println("\nTerminating Program All Work Completed")
	}
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
