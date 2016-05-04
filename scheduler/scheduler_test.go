package main

import "fmt"
import "sync"
import "testing"

import ci "scootdev/scoot/clusterimplementations"
import cm "scootdev/scoot/clustermembership"
import distributor "scootdev/scoot/distributor"

func runTasks(
	work map[string]int,
	workCh chan string,
	cluster cm.Cluster,
	distributor distributor.Distributor) {

	//schedule and execute all the work
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for msg, _ := range work {
			workCh <- msg
		}
		close(workCh)
		wg.Done()
	}()

	go func() {
		scheduleWork(workCh, cluster, distributor)
		wg.Done()
	}()

	wg.Wait()
}

func generateTestTasks(numTasks int) map[string]int {
	work := make(map[string]int)
	for x := 0; x < numTasks; x++ {
		msg := fmt.Sprintf("Task %d", x)
		work[msg] = 0
	}

	return work
}

func verifyTasksRanOnce(work map[string]int, cluster cm.Cluster, t *testing.T) {
	for _, node := range cluster.Members() {

		var tNode *ci.TestNode
		tNode = node.(*ci.TestNode)

		//mark msgs as having been processed
		for _, msg := range tNode.MsgsReceived {

			numExecuted, ok := work[msg]
			if !ok {
				t.Error(fmt.Sprintf("node %s, processed unknown msg %s", tNode.Id(), msg))
			} else {
				work[msg] = numExecuted + 1
			}
		}
	}

	//Verify all messages were processed exactly once
	for msg, numExecuted := range work {
		if numExecuted == 0 {
			t.Error(fmt.Sprintf("msg: %s, was not executed", msg))
		}

		if numExecuted > 1 {
			t.Error(fmt.Sprintf("msg: %s, was executed %d times, expected only once", msg, numExecuted))
		}
	}
}

func TestSchduleRandomTasks(t *testing.T) {
	//Set up test cluster and test work
	//code assumes that numTasks % numNodes = 0
	numTasks := 100000
	numNodes := 100

	cluster := ci.StaticTestNodeClusterFactory(numNodes)

	workCh := make(chan string)
	distributor := &distributor.Random{}
	work := generateTestTasks(numTasks)

	runTasks(work, workCh, cluster, distributor)
	verifyTasksRanOnce(work, cluster, t)
}

func TestScheduleRoundRobinTasks(t *testing.T) {

	//Set up test cluster and test work
	//code assumes that numTasks % numNodes = 0
	numTasks := 100000
	numNodes := 100

	cluster := ci.StaticTestNodeClusterFactory(numNodes)

	workCh := make(chan string)
	distributor := &distributor.RoundRobin{}
	work := generateTestTasks(numTasks)

	runTasks(work, workCh, cluster, distributor)

	verifyTasksRanOnce(work, cluster, t)

	//verify load was distributed evenly
	for _, node := range cluster.Members() {

		var tNode *ci.TestNode
		tNode = node.(*ci.TestNode)

		//ensure the correct number of msgs were distributed to this node
		if len(tNode.MsgsReceived) != (numTasks / numNodes) {
			t.Error(fmt.Sprintf(
				"Did Not Distribute Load Correctly, expected %d, actual %d for node %s",
				numTasks/numNodes,
				len(tNode.MsgsReceived),
				tNode.Id()))
		}
	}
}
