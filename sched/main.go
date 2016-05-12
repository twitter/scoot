package main

import "fmt"
import "sync"

import ci "scootdev/scoot/sched/clusterimplementations"
import cm "scootdev/scoot/sched/clustermembership"
import distributor "scootdev/scoot/sched/distributor"

func main() {

	cluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.Members())

	workCh := make(chan string)
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
	fmt.Println("\nTerminating Program All Work Completed")
}

func scheduleWork(
	workCh <-chan string,
	cluster cm.Cluster,
	distributor distributor.Distributor) {

	var wg sync.WaitGroup
	for work := range workCh {
		node := distributor.DistributeWork(work, cluster)

		wg.Add(1)
		go func(w string, n cm.Node) {
			defer wg.Done()
			//Todo: error handling, what if request fails
			n.SendMessage(w)
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
func generateTasks(work chan<- string, numTasks int) {

	for x := 0; x < numTasks; x++ {
		work <- fmt.Sprintf("Task %d", x)
	}
	close(work)
}
