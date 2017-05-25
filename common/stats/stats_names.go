package stats

/*
This file defines all the metrics being collected.   As new metrics are added please follow this pattern.
 */

const (
	/*** Node availability (ClusterManger) metrics ***/
	/*
	the number of worker nodes that are available or running tasks (not suspended)
	 */
	ClusterAvailableNodes = "availableNodes"

	/*
	the number of idle worker nodes (available, but not running a task)
	 */
	ClusterIdleNodes ="idleNodes"

	/*
	the number of lost worker nodes (not responding to status requests)
	 */
	ClusterLostNodes ="lostNodes"


	/*** Scheduler Metrics ***/
	/*
	The number of jobs in the inProgress list at the end of each time through the
	scheduler's job handling loop
	 */

	SchedAcceptedJobsGauge="schedAcceptedJobsGauge"

	/*
	the number of tasks that have finished (including those that have been killed)
	 */
	SchedCompletedTaskCounter = "completedTaskCounter"

	/*
	the number of times the processing failed to serialize the workerapi status object
	 */
	SchedFailedTaskSerializeCounter = "failedTaskSerializeCounter"
	/*
	The number of tasks from the inProgress list waiting to start or running.
	*/
	SchedInProgressTasksGauge="schedInProgressTasksGauge"

	/*
	the number of job requests that have been put on the addJobChannel
	 */
	SchedJobsCounter="schedJobsCounter"

	/* TODO - remove? (discuss usefulness)
	the amount of time it takes to verify a job definition, it to the add job channel and
	return a job id.
	 */
	SchedJobLatency_ms         = "schedJobLatency_ms"

	/* TODO - remove? (discuss usefulness)
	the number of jobs requests that have been able to be converted from the thrift
	request to the sched.JobDefinition structure.
	 */
	SchedJobRequestsCounter    = "schedJobRequestsCounter"

	/*
	the number of running tasks.  Collected at the end of each time through the
	scheduler's job handling loop.
	 */
	SchedNumRunningTasksGauge="schedNumRunningTasksGauge"

	/*
	the number of times the platform retried sending an end saga message
	 */
	SchedRetriedEndSagaCounter= "schedRetriedEndSagaCounter"

	/*
	The number of jobs waiting to start in the inProgress list at the end of each time through the
	scheduler's job handling loop.  (No tasks in this job have been started.)
	 */
	SchedWaitingJobsGauge="schedWaitingJobsGauge"

)
