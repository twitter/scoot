package stats

/*
This file defines all the metrics being collected.   As new metrics are added please follow this pattern.
*/

const (
	/****************** ClusterManger metrics ***************************/
	/*
		Cluster metrics on Node types
		Available - available or running tasks (not suspended)
		Free - available, not running
		Running - running tasks
		Lost - not responding to status requests
	*/
	ClusterAvailableNodes = "availableNodes"
	ClusterFreeNodes      = "freeNodes"
	ClusterRunningNodes   = "runningNodes"
	ClusterLostNodes      = "lostNodes"

	/************************* Bundlestore metrics **************************/
	/*
		Bundlestore download metrics (Reads/Gets from top-level Bundlestore/Apiserver)
	*/
	BundlestoreDownloadLatency_ms = "downloadLatency_ms"
	BundlestoreDownloadCounter    = "downloadCounter"
	BundlestoreDownloadErrCounter = "downloadErrCounter"
	BundlestoreDownloadOkCounter  = "downloadOkCounter"

	/*
		Bundlestore upload metrics (Writes/Puts to top-level Bundlestore/Apiserver)
	*/
	BundlestoreUploadCounter         = "uploadCounter"
	BundlestoreUploadErrCounter      = "uploadErrCounter"
	BundlestoreUploadExistingCounter = "uploadExistingCounter"
	BundlestoreUploadLatency_ms      = "uploadLatency_ms"
	BundlestoreUploadOkCounter       = "uploadOkCounter"

	/*
	   Bundlestore request counters and uptime statistics
	*/
	BundlestoreRequestCounter     = "serveRequestCounter"
	BundlestoreRequestOkCounter   = "serveOkCounter"
	BundlestoreServerStartedGauge = "bundlestoreStartGauge"
	BundlestoreUptime_ms          = "bundlestoreUptimeGauge_ms"

	/************************* Groupcache Metrics ***************************/
	/*
		Groupcache Read metrics
	*/
	GroupcacheReadCounter    = "readCounter"
	GroupcacheReadOkCounter  = "readOkCounter"
	GroupcacheReadLatency_ms = "readLatency_ms"

	/*
		Groupcache Exists metrics
	*/
	GroupcacheExistsCounter   = "existsCounter"
	GroupcachExistsLatency_ms = "existsLatency_ms"
	GroupcacheExistsOkCounter = "existsOkCounter"

	/*
		Groupcache Write metrics
	*/
	GroupcacheWriteCounter    = "writeCounter"
	GroupcacheWriteOkCounter  = "writeOkCounter"
	GroupcacheWriteLatency_ms = "writeLatency_ms"

	/*
		Groupcache Underlying load metrics (cache miss loads)
	*/
	GroupcacheReadUnderlyingCounter    = "readUnderlyingCounter"
	GroupcacheReadUnderlyingLatency_ms = "readUnderlyingLatency_ms"

	/*
		Groupcache library - per-cache metrics (typical groupcache includes separate "main" and "hot" caches)
	*/
	GroupcacheMainBytesGauge       = "mainBytesGauge"
	GroupcacheMainGetsCounter      = "mainGetsCounter"
	GroupcacheMainHitsCounter      = "mainHitsCounter"
	GroupcacheMainItemsGauge       = "mainItemsGauge"
	GroupcacheMainEvictionsCounter = "mainEvictionsCounter"
	GroupcacheHotBytesGauge        = "hotBytesGauge"
	GroupcacheHotGetsCounter       = "hotGetsCounter"
	GroupcacheHotHitsCounter       = "hotHitsCounter"
	GroupcacheHotItemsGauge        = "hotItemsGauge"
	GroupcacheHotEvictionsCounter  = "hotEvictionsCounter"

	/*
		Groupcache library - per-group metrics (overall metrics for a groupcache on a single Apiserver)
	*/
	GroupcacheGetCounter              = "cacheGetCounter"
	GroupcacheHitCounter              = "cacheHitCounter"
	GroupcacheLoadCounter             = "cacheLoadCounter"
	GroupcacheIncomingRequestsCounter = "cacheIncomingRequestsCounter"
	GroupcacheLocalLoadErrCounter     = "cacheLocalLoadErrCounter"
	GroupcacheLocalLoadCounter        = "cacheLocalLoadCounter"
	GroupcachePeerGetsCounter         = "cachePeerGetsCounter"
	GroupcachPeerErrCounter           = "cachePeerErrCounter"

	/*
		Groupcache peer pool metrics (maintained by Scoot)
	*/
	GroupcachePeerCountGauge       = "peerCountGauge"
	GroupcachePeerDiscoveryCounter = "peerDiscoveryCounter"

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

	/*
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

	/*
		the amount of time it takes to verify a job definition, add it to the job channel and
		return a job id.
	*/
	SchedJobLatency_ms = "schedJobLatency_ms"

	/*
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

	/****************************** Worker/Invoker Execution Timings ***************************/
	/*
		Execution metadata timing metrics emitted by Worker.
		These probably aren't updated enough to make use of the Histogram values, but
		using that type results in these values being automatically cleared each
		stat interval vs a gauge.
	*/
	BzExecQueuedTimeHistogram_ms           = "bzExecQueuedTimeHistogram_ms"
	BzExecInputFetchTimeHistogram_ms       = "bzExecInputFetchTimeHistogram_ms"
	BzExecActionCacheCheckTimeHistogram_ms = "BzExecActionCacheCheckTimeHistogram_ms"
	BzExecActionFetchTimeHistogram_ms      = "BzExecActionFetchTimeHistogram_ms"
	BzExecCommandFetchTimeHistogram_ms     = "BzExecCommandFetchTimeHistogram_ms"
	BzExecExecerTimeHistogram_ms           = "bzExecExecerTimeHistogram_ms"

	/****************************** CAS Service ******************************************/
	/*
		FindMissingBlobs API metrics emitted by Apiserver
	*/
	BzFindBlobsSuccessCounter  = "bzFindBlobsSuccessCounter"
	BzFindBlobsFailureCounter  = "bzFindBlobsFailureCounter"
	BzFindBlobsLengthHistogram = "bzFindBlobsLengthHistogram"
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
		ActionCache result metrics
	*/
	BzCachedExecCounter = "bzCachedExecCounter"

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
