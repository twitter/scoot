package main

import "fmt"
import "sync"

import ci "scootdev/scoot/clusterimplementations"
import cm "scootdev/scoot/clustermembership"
import distributor "scootdev/scoot/distributor"

func main() {

	cluster := ci.StaticLocalNodeClusterFactory(10)
	fmt.Println("clusterMembers:", cluster.Members())

	work := make(chan string)
	distributor := distributor.RoundRobin{}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		generateTasks(work, 100)
		wg.Done()
	}()

	go func() {
		scheduleWork(work, cluster, &distributor)
		wg.Done()
	}()

	wg.Wait()
	fmt.Println("\nTerminating Program All Work Completed")
}

func scheduleWork(
	work <-chan string,
	cluster cm.Cluster,
	distributor distributor.Distributor) {
	for w := range work {
		node := distributor.DistributeWork(w, cluster)
		//Todo: error handling, what if request fails
		node.SendMessage(w)
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
func generateTasks(work chan<- string, numTasks int) {

	for x := 0; x < numTasks; x++ {
		work <- fmt.Sprintf("Task %d", x)
	}
	close(work)
}
