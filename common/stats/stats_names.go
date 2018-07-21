package stats

/*
This file defines all the metrics being collected.   As new metrics are added please follow this pattern.
*/

const (
	/************************* Bundlestore metrics **************************/
	//TODO - verify all the bundlestore descriptions
	/*
		the number of groupcache peers the bundlestore sees
	*/
	BundlestoreGroupcachePeerCountGauge = "peerCountGauge"

	/*
		the number of times bundlestore is discovering how many peers it has
	*/
	BundlestoreGroupcachePeerDiscoveryCounter = "peerDiscoveryCounter"

	/*
		the number of times bundlestore is reading from its underlying groupcache
	*/
	BundlestoreGroupcacheReadUnderlyingCounter = "readUnderlyingCounter"

	/*
		amount of time it takes to download a bundlestore (to the worker_ (note: it includes downloads that errored)
	*/
	BundlestoreDownloadLatency_ms = "downloadLatency_ms"

	/*
		number of times bundlestore tries to download a snapshot (to a worker) (includes downloads that errored
	*/
	BundlestoreDownloadCounter = "downloadCounter"

	/*
		number of times the bundlestore download (to the worker) failed
	*/
	BundlestoreDownloadErrCounter = "downloadErrCounter"

	/*
		number of times the bundlestore download (to the worker) succeeded
	*/
	BundlestoreDownloadOkCounter = "downloadOkCounter"

	/*
	   bundlestore service request
	*/
	BundlestoreRequestCounter = "serveRequestCounter"

	/*
	   End of processing any bundlestore service request
	*/
	BundlestoreRequestOkCounter = "serveOkCounter"

	/*
		record the start of the bundlestore server
	*/
	BundlestoreServerStartedGauge = "bundlestoreStartGauge"

	/*
		number of times a snapshot was uploaded to the bundlestore service
	*/
	BundlestoreUploadCounter = "uploadCounter"

	/*
		the number of bundlestore uploads that errored
	*/
	BundlestoreUploadErrCounter = "uploadErrCounter"

	/*
		number of times an upload is trying to overwrite an existing one
	*/
	BundlestoreUploadExistingCounter = "uploadExistingCounter"

	/*
		time to upload a snapshot to a bundlestore service
	*/
	BundlestoreUploadLatency_ms = "uploadLatency_ms"

	/*
		the number of bundlestore uploads that were successful
	*/
	BundlestoreUploadOkCounter = "uploadOkCounter"

	/*
		The length of time the server has been running
	*/
	BundlestoreUptime_ms = "bundlestoreUptimeGauge_ms"

	/****************** ClusterManger metrics ***************************/
	/*
		the number of worker nodes that are available or running tasks (not suspended)
	*/
	ClusterAvailableNodes = "availableNodes"

	/*
		the number of free worker nodes (available, but not running a task)
	*/
	ClusterFreeNodes = "freeNodes"

	/*
		the number of running worker nodes (running + free + suspended ~= allNodes (may lag))
	*/
	ClusterRunningNodes = "runningNodes"

	/*
		the number of lost worker nodes (not responding to status requests)
	*/
	ClusterLostNodes = "lostNodes"

	/************************* Groupcache Metrics ***************************/ // TODO verify/update all groupcache descriptions
	/*
		the number of time groupcache tried to determine if a bundle existed
	*/
	GroupcacheExistsCounter = "existsCounter"

	/*
		the amount of time it takes to determine if a bundlestore exists
	*/
	GroupcachExistsLatency_ms = "existsLatency_ms"

	/*
		the number of exists requests that returned true
	*/
	GroupcacheExistsOkCounter = "existsOkCounter"

	/*
		the number of gets directed to the groupcache
	*/
	GroupcacheGetCounter = "cacheGetCounter"

	/*
		the number of times the requested bundle was found in this groupcache
	*/
	GroupcacheHitCounter = "cacheHitCounter"

	/*
		?
	*/
	GroupcacheHotBytesGauge = "hotBytesGauge"

	/*
		?
	*/
	GroupcacheHotGetsCounter = "hotGetsCounter"

	/*
		?
	*/
	GroupcacheHotHitsCounter = "hotHitsCounter"

	/*
		?
	*/
	GroupcacheHotItemsGauge = "hotItemsGauge"

	/*
		?
	*/
	GroupcacheIncomingRequestsCounter = "cacheIncomingRequestsCounter"

	/*
		?
	*/
	GroupcacheLocalLoadErrCounter = "cacheLocalLoadErrCounter"

	/*
		?
	*/
	GroupcacheLocalLoadCounter = "cacheLocalLoadCounter"

	/*
		?the size of the bundles managed by this instance of the service
	*/
	GroupcacheMainBytesGauge = "mainBytesGauge"

	/*
		?the number of times the service tried to find a bundle in its cache?
	*/
	GroupcacheMainGetsCounter = "mainGetsCounter"

	/*
		?the number of times the requested bundle was found in the service
	*/
	GroupcacheMainHitsCounter = "mainHitsCounter"

	/*
		?the number of items managed by this instance of the service
	*/
	GroupcacheMainItemsGauge = "mainItemsGauge"

	/*
		the number of times the groupcache tried to get a bundle from its peers
	*/
	GroupcachePeerGetsCounter = "cachePeerGetsCounter"

	/*
		the number of errors encountered trying to interact with a peer
	*/
	GroupcachPeerErrCounter = "cachePeerErrCounter"

	/*
		the number of attempted bundle reads for the specific bundlestore instande
	*/
	GroupcacheReadCounter = "readCounter"

	/*
		the number of successful bundle reads for the specific bundlestore instance
	*/
	GroupcacheReadOkCounter = "readOkCounter"

	/*
		the time to read a bundle from groupcache ?includes blob store as well as memory retrieval?
	*/
	GroupcacheReadLatency_ms = "readLatency_ms"

	/*
		the number of write requests to groupcache
	*/
	GroupcacheWriteCounter = "writeCounter"
	/*
		the number of successful writes to groupcache
	*/
	GroupcacheWriteOkCounter = "writeOkCounter"

	/*
		the amount of time it took groupcache to write the bundle
	*/
	GroupcacheWriteLatency_ms = "writeLatency_ms"

	/****************************** Scheduler Metrics ****************************************/
	/*
		The number of jobs in the inProgress list at the end of each time through the
		scheduler's job handling loop
	*/

	SchedAcceptedJobsGauge = "schedAcceptedJobsGauge"

	/*
		the number of tasks that have finished (including those that have been killed)
	*/
	SchedCompletedTaskCounter = "completedTaskCounter"

	/* TODO - remove? (discuss usefulness)
	The number of times any of the following conditions occurred:
	- the task's command errored while running
	- the platform could not run the task
	- the platform encountered an error reporting the end of the task to saga
	*/
	SchedFailedTaskCounter = "failedTaskCounter"

	/*
		the number of times the processing failed to serialize the workerapi status object
	*/
	SchedFailedTaskSerializeCounter = "failedTaskSerializeCounter"
	/*
		The number of tasks from the inProgress list waiting to start or running.
	*/
	SchedInProgressTasksGauge = "schedInProgressTasksGauge"

	/*
		the number of job requests that have been put on the addJobChannel
	*/
	SchedJobsCounter = "schedJobsCounter"

	/* TODO - remove? (discuss usefulness)
	the amount of time it takes to verify a job definition, add it to the job channel and
	return a job id.
	*/
	SchedJobLatency_ms = "schedJobLatency_ms"

	/* TODO - remove? (discuss usefulness)
	the number of jobs requests that have been able to be converted from the thrift
	request to the sched.JobDefinition structure.
	*/
	SchedJobRequestsCounter = "schedJobRequestsCounter"

	/*
		the number of jobs with tasks running.  Only reported by requestor
	*/
	SchedNumRunningJobsGauge = "schedNumRunningJobsGauge"

	/*
		the number of running tasks.  Collected at the end of each time through the
		scheduler's job handling loop.
	*/
	SchedNumRunningTasksGauge = "schedNumRunningTasksGauge"

	/*
		the number of tasks waiting to start. (Only reported by requestor)
	*/
	SchedNumWaitingTasksGauge = "schedNumWaitingTasksGauge"

	/*
		the number of active tasks that stopped because they were preempted by the scheduler
	*/
	SchedPreemptedTasksCounter = "preemptedTasksCounter"

	/*
		the number of jobs with priority 0
	*/
	SchedPriority0JobsGauge = "priority0JobsGauge"

	/*
		the number of jobs with priority 1
	*/
	SchedPriority1JobsGauge = "priority1JobsGauge"

	/*
		the number of jobs with priority 2
	*/
	SchedPriority2JobsGauge = "priority2JobsGauge"

	/*
		the number of times the platform retried sending an end saga message
	*/
	SchedRetriedEndSagaCounter = "schedRetriedEndSagaCounter"

	/*
		record the start of the scheduler server
	*/
	SchedServerStartedGauge = "schedStartGauge"

	/*
		the number of tasks that were scheduled to be started on nodes with the last
		run of the scheduling algorithm
		TODO- verify the description with Jason
	*/
	SchedScheduledTasksCounter = "scheduledTasksCounter"

	/*
		the number of times the server received a job kill request
	*/
	SchedServerJobKillCounter = "jobKillRpmCounter"

	/*
		the amount of time it took to kill a job (from the server)
	*/
	SchedServerJobKillLatency_ms = "jobKillLatency_ms"

	/*
		the number of job status requests the thrift server received
	*/
	SchedServerJobStatusCounter = "jobStatusRpmCounter"

	/*
		the amount of time it took to process a job status request (from the server)
	*/
	SchedServerJobStatusLatency_ms = "jobStatusLatency_ms"

	/*
		the number of job run requests the thrift server received
	*/
	SchedServerRunJobCounter = "runJobRpmCounter"

	/*
		the amount of time it took to process a RunJob request (from the server) and return either an error or job id
	*/
	SchedServerRunJobLatency_ms = "runJobLatency_ms"

	/*
		The amount of time it takes to assign the tasks to nodes
	*/
	SchedTaskAssignmentsLatency_ms = "schedTaskAssignmentsLatency_ms"

	/*
		the number of times the task runner had to retry the task start
	*/
	SchedTaskStartRetries = "taskStartRetries"

	/*
		The length of time the server has been running
	*/
	SchedUptime_ms = "schedUptimeGauge_ms"

	/*
		The number of jobs waiting to start in the inProgress list at the end of each time through the
		scheduler's job handling loop.  (No tasks in this job have been started.)
	*/
	SchedWaitingJobsGauge = "schedWaitingJobsGauge"

	/*
		Amount of time it takes the scheduler to complete a full step()
	*/
	SchedStepLatency_ms = "schedStepLatency_ms"

	/******************************** Worker metrics **************************************/
	/*
		The number of runs the worker has currently running
	*/
	WorkerActiveRunsGauge = "activeRunsGauge"

	/*
		the number of times the worker downloaded a snapshot from bundlestore
	*/
	WorkerDownloads = "workerDownloads"

	/*
		the number of times a worker's inti failed.  Should be at most 1 for each worker
	*/
	WorkerDownloadInitFailure = "workerDownloadInitFailure"

	/*
		the amount of time spent downloading snapshots to the worker.  This includes time for
		successful as well as erroring downloads
	*/
	WorkerDownloadLatency_ms = "workerDownloadLatency_ms"

	/*
		The number of runs in the worker's statusAll() response that are not currently running
		TODO - this includes runs that are waiting to start - will not be accurate if we go to a
		worker that can run multiple commands
	*/
	WorkerEndedCachedRunsGauge = "endedCachedRunsGauge"

	/*
		The number of runs that the worker tried to run an whose state is failed
		TODO - understand how/when this gets reset - it's based on the runs in the worker's StatusAll()
		response - how/when do old jobs drop out of StatusAll()?
	*/
	WorkerFailedCachedRunsGauge = "failedCachedRunsGauge"

	/*
		The amount of time it took a worker to init
	*/
	WorkerFinalInitLatency_ms = "workerFinishedInitLatency_ms"

	/*
		The number of workers who are currently exceeding the max init time
	*/
	WorkerActiveInitLatency_ms = "workerActiveInitLatency_ms"

	/*
		the amount of worker's memory currently consumed by the current command (and its subprocesses)
		TODO- verify with Ryan that this description is correct
		scope is osexecer - change to worker?
	*/
	WorkerMemory = "memory"

	/*
		the number of abort requests received by the worker
	*/
	WorkerServerAborts = "aborts"

	/*
		the number of clear requests received by the worker
	*/
	WorkerServerClears = "clears"

	/*
		The number of QueryWorker requests received by the worker server
	*/
	WorkerServerQueries = "workerQueries"

	/*
		the amount of time it takes the worker to put a run request on the worker's run queue
	*/
	WorkerServerStartRunLatency_ms = "runLatency_ms"

	/*
		the number of run requests a worker has received
	*/
	WorkerServerRuns = "runs"

	/*
		record when a worker service is starting
	*/
	WorkerServerStartedGauge = "workerStartGauge"

	/*
		The time it takes to run the task (including snapshot handling)
	*/
	WorkerTaskLatency_ms = "workerTaskLatency_ms"

	/*
		Time since the most recent run, status, abort, erase request
	*/
	WorkerTimeSinceLastContactGauge_ms = "timeSinceLastContactGauge_ms"

	/*
		the number of times the worker uploaded a snapshot to bundlestore
	*/
	WorkerUploads = "workerUploads"

	/*
		the amount of time spent uploading snapshots to bundlestore.  This includes time for
		successful as well as erroring uploads
	*/
	WorkerUploadLatency_ms = "workerUploadLatency_ms"

	/*
		Time since the worker started
	*/
	WorkerUptimeGauge_ms = "workerUptimeGauge_ms"

	/****************************** Git Metrics **********************************************/
	/*
		The number of failures trying to init a ref clone
	*/
	GitClonerInitFailures = "clonerInitFailures"

	/*
		The amount of time it took to init a ref clone
	*/
	GitClonerInitLatency_ms = "clonerInitLatency_ms"

	/*
		The number of times a gitfiler.Checkouter Checkout had to resort to a git fetch
	*/
	GitFilerCheckoutFetches = "gitfilerCheckoutFetches"

	/*
		The number of times a gitdb stream backend had to resort to a git fetch
	*/
	GitStreamUpdateFetches = "gitStreamUpdateFetches"

	/****************************** Bazel Metrics **********************************************/

	/****************************** Execution Service ******************************************/
	/*
		Execute API metrics emitted by Scheduler
	*/
	BzExecSuccessCounter = "bzExecSuccessCounter"
	BzExecFailureCounter = "bzExecFailureCounter"
	BzExecLatency_ms     = "bzExecLatency_ms"

	/*
		Longrunning GetOperation API metrics emitted by Scheduler
	*/
	BzGetOpSuccessCounter = "bzGetOpSuccessCounter"
	BzGetOpFailureCounter = "bzGetOpFailureCounter"
	BzGetOpLatency_ms     = "bzGetOpLatency_ms"

	/*
		Execution metadata timing metrics emitted by Worker
	*/
	BzExecQueuedTimeHistogram_ms = "bzExecQueuedTimeHistogram_ms"
	BzExecExecerTimeHistogram_ms = "bzExecExecerTimeHistogram_ms"

	/****************************** CAS Service ******************************************/
	/*
		FindMissingBlobs API metrics emitted by Apiserver
	*/
	BzFindBlobsSuccessCounter  = "bzFindBlobsSuccessCounter"
	BzFindBlobsFailureCounter  = "bzFindBlobsFailureCounter"
	BzFindBlobsLengthHistogram = "bzFindlobsLengthHistogram"
	BzFindBlobsLatency_ms      = "bzFindBlobsLatency_ms"

	/*
		CAS Read API metrics emitted by Apiserver
	*/
	BzReadSuccessCounter = "bzReadSuccessCounter"
	BzReadFailureCounter = "bzReadFailureCounter"
	BzReadBytesHistogram = "bzReadBytesHistogram"
	BzReadLatency_ms     = "bzReadLatency_ms"

	/*
		CAS Write API metrics emitted by Apiserver
	*/
	BzWriteSuccessCounter = "bzWriteSuccessCounter"
	BzWriteFailureCounter = "bzWriteFailureCounter"
	BzWriteBytesHistogram = "bzWriteBytesHistogram"
	BzWriteLatency_ms     = "bzWriteLatency_ms"

	/****************************** ActionCache Service ****************************************/
	/*
		GetActionResult API metrics emitted by Apiserver
	*/
	BzGetActionSuccessCounter = "bzGetActionSuccessCounter"
	BzGetActionFailureCounter = "bzGetActionFailureCounter"
	BzGetActionLatency_ms     = "bzGetActionLatency_ms"

	/*
		UpdateActionResult API metrics emitted by Apiserver
	*/
	BzUpdateActionSuccessCounter = "bzUpdateActionSuccessCounter"
	BzUpdateActionFailureCounter = "bzUpdateActionFailureCounter"
	BzUpdateActionLatency_ms     = "bzUpdateActionLatency_ms"
)
