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

	ClusterNodeUpdateFreqMs = "clusterSetNodeUpdatesFreq_ms"
	ClusterFetchFreqMs      = "clusterFetchFreq_ms"
	ClusterFetchDurationMs  = "clusterFetchDuration_ms"
	ClusterNumFetchedNodes  = "clusterNumFetchedNodes"
	ClusterFetchedError     = "clusterFetchError"

	/************************* Bundlestore metrics **************************/
	/*
		Bundlestore download metrics (Reads/Gets from top-level Bundlestore/Apiserver)
	*/
	BundlestoreDownloadLatency_ms = "downloadLatency_ms"
	BundlestoreDownloadCounter    = "downloadCounter"
	BundlestoreDownloadErrCounter = "downloadErrCounter"
	BundlestoreDownloadOkCounter  = "downloadOkCounter"

	/*
		Bundlestore check metrics (Exists/Heads from top-level Bundlestore/Apiserver)
	*/
	BundlestoreCheckLatency_ms = "checkLatency_ms"
	BundlestoreCheckCounter    = "checkCounter"
	BundlestoreCheckErrCounter = "checkErrCounter"
	BundlestoreCheckOkCounter  = "checkOkCounter"

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
		Groupcache Underlying load metrics (cache misses)
	*/
	GroupcacheReadUnderlyingCounter     = "readUnderlyingCounter"
	GroupcacheReadUnderlyingLatency_ms  = "readUnderlyingLatency_ms"
	GroupcacheExistUnderlyingCounter    = "existUnderlyingCounter"
	GroupcacheExistUnderlyingLatency_ms = "existUnderlyingLatency_ms"
	GroupcacheWriteUnderlyingCounter    = "writeUnderlyingCounter"
	GroupcacheWriteUnderlyingLatency_ms = "writeUnderlyingLatency_ms"

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
	GroupcacheContainCounter          = "cacheContainCounter"
	GroupcachePutCounter              = "cachePutCounter"
	GroupcacheHitCounter              = "cacheHitCounter"
	GroupcacheLoadCounter             = "cacheLoadCounter"
	GroupcacheCheckCounter            = "cacheCheckCounter"
	GroupcacheStoreCounter            = "cacheStoreCounter"
	GroupcacheIncomingRequestsCounter = "cacheIncomingRequestsCounter"
	GroupcacheLocalLoadErrCounter     = "cacheLocalLoadErrCounter"
	GroupcacheLocalLoadCounter        = "cacheLocalLoadCounter"
	GroupcacheLocalCheckErrCounter    = "cacheLocalCheckErrCounter"
	GroupcacheLocalCheckCounter       = "cacheLocalCheckCounter"
	GroupcacheLocalStoreErrCounter    = "cacheLocalStoreErrCounter"
	GroupcacheLocalStoreCounter       = "cacheLocalStoreCounter"
	GroupcachePeerGetsCounter         = "cachePeerGetsCounter"
	GroupcachePeerChecksCounter       = "cachePeerChecksCounter"
	GroupcachePeerPutsCounter         = "cachePeerPutsCounter"
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
		the number of async runners still waiting on task completion
	*/
	SchedNumAsyncRunnersGauge = "schedNumAsyncRunnersGauge"

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

	/*
		Amount of time it takes the scheduler to add newly requested jobs to list of jobs currently being handled by scheduler
	*/
	SchedAddJobsLatency_ms = "schedAddJobsLatency_ms"

	/*
		Amount of time it takes the scheduler to check newly requested jobs for validity
	*/
	SchedCheckJobsLoopLatency_ms = "schedCheckJobsLoopLatency_ms"

	/*
		Amount of time it takes the scheduler to add newly verified jobs from add job channel to list of jobs currently being handled by scheduler
	*/
	SchedAddJobsLoopLatency_ms = "schedAddJobsLoopLatency_ms"

	/*
		Amount of time it takes the scheduler to update list of removed / added nodes to its worker cluster
	*/
	SchedUpdateClusterLatency_ms = "schedUpdateClusterLatency_ms"

	/*
		Amount of time it takes the scheduler to process all messages in mailbox & execute callbacks (if applicable)
	*/
	SchedProcessMessagesLatency_ms = "schedProcessMessagesLatency_ms"

	/*
		Amount of time it takes the scheduler to check if any of the in progress jobs are completed
	*/
	SchedCheckForCompletedLatency_ms = "schedCheckForCompletedLatency_ms"

	/*
		Amount of time it takes the scheduler to kill all jobs requested to be killed
	*/
	SchedKillJobsLatency_ms = "schedKillJobsLatency_ms"

	/*
		Amount of time it takes the scheduler to figure out which tasks to schedule next and on which worker
	*/
	SchedScheduleTasksLatency_ms = "schedScheduleTasksLatency_ms"

	/*--------------------- load based scheduler stats ---------------------------*/
	/*
		number of time Load Based Scheduler saw an unrecognized requestor
	*/
	SchedLBSUnknownJobCounter = "schedLBSUnknownJobCounter"

	/*
	   number of jobs ignored because the load % is 0
	*/
	SchedLBSIgnoredJobCounter = "schedLBSIgnoredJobCounter"

	/*
		number of tasks starting by job class (after the last run of lbs)
	*/
	SchedJobClassTasksStarting = "schedStartingTasks_"

	/*
		number of tasks already running by job class (before starting tasks as per lbs)
	*/
	SchedJobClassTasksRunning = "schedRunningTasks_"

	/*
		number of tasks still waiting by job class after the tasks identified by lbs have started
	*/
	SchedJobClassTasksWaiting = "schedWaitingTasks_"

	/*
		job class % (set via scheduler api)
	*/
	SchedJobClassDefinedPct = "schedClassTargetPct_"

	/*
		job class actual % (set computed from running tasks)
	*/
	SchedJobClassActualPct = "schedClassActualPct_"

	/*
		number of tasks being stopped for the class (due to rebalancing)
	*/
	SchedStoppingTasks = "schedStoppingTasks_"

	/*
		scheduler internal data structure size monitoring
	*/
	SchedLBSConfigLoadPercentsSize     = "schedDS_size_ConfigLoadPercents"
	SchedLBSConfigRequestorToPctsSize  = "schedDS_size_ConfigRequestorToClassMap"
	SchedLBSConfigDescLoadPctSize      = "schedDS_size_ConfigDescLoadPercents"
	SchedLBSWorkingJobClassesSize      = "schedDS_size_WorkingJobClasses"
	SchedLBSWorkingLoadPercentsSize    = "schedDS_size_WorkingLoadPercents"
	SchedLBSWorkingRequestorToPctsSize = "schedDS_size_WorkingRequestorToClassMap"
	SchedLBSWorkingDescLoadPctSize     = "schedDS_size_WorkingDescLoadPercents"
	SchedTaskStartTimeMapSize          = "schedDS_size_taskStartTimeMap"
	SchedInProgressJobsSize            = "schedDS_size_inProgressJobs"
	SchedRequestorMapSize              = "schedDS_size_requestorMap"
	SchedRequestorHistorySize          = "schedDS_size_requestorHistory"
	SchedTaskDurationsSize             = "schedDS_size_taskDurations"
	SchedSagasSize                     = "schedDS_size_sagas"
	SchedRunnersSize                   = "schedDS_size_runners"

	/******************************** Worker metrics **************************************/
	/*
		The number of runs the worker has currently running
	*/
	WorkerActiveRunsGauge = "activeRunsGauge"

	/*
		The disk size change for the indicated directory seen when running the task.
		The reported stat will be of the form commandDirUsage_kb_<PathSuffix from
	*/
	CommandDirUsageKb = "commandDirUsage_kb"

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
		record high memory utilization on task start
	*/
	WorkerHighInitialMemoryUtilization = "highInitialMemoryUtilization"

	/*
		record when worker memory consumption exceed soft memory cap
	*/
	WorkerMemoryCapExceeded = "memoryCapExceeded"

	/*
		A gauge used to indicate if the worker is currently running a task or if is idling
	*/
	WorkerRunningTask = "runningTask"

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
		record when a worker service becomes unhealthy
	*/
	WorkerUnhealthy = "workerUnhealthy"

	/*
		The time it takes to run the task (including snapshot handling)
	*/
	WorkerTaskLatency_ms = "workerTaskLatency_ms"

	/*
		Time since the most recent run, status, abort, erase request
	*/
	WorkerTimeSinceLastContactGauge_ms = "timeSinceLastContactGauge_ms"

	/*
		the number of times the worker timed out running a command
	*/
	WorkerTimeouts = "workerTimeouts"

	/*
		the number of times the worker uploaded a log(stdout/stderr/stdlog) to storage
	*/
	WorkerUploads = "workerUploads"

	/*
		the number of times the worker log(stdout/stderr/stdlog) upload failed
	*/
	WorkerLogUploadFailures = "workerLogUploadFailures"

	/*
		the number of times the worker log(stdout/stderr/stdlog) upload timed out
	*/
	WorkerLogUploadTimeouts = "workerLogUploadTimeouts"

	/*
		the number of times the worker log(stdout/stderr/stdlog) upload was retried
	*/
	WorkerLogUploadRetries = "workerLogUploadRetries"

	/*
		the amount of time spent uploading the log to storage. This includes time for
		successful as well as erroring uploads
	*/
	WorkerLogUploadLatency_ms = "workerLogUploadLatency_ms"

	/*
		the overall time spent uploading all three logs(stderr, stdout & stdlog)
	*/
	WorkerUploadLatency_ms = "workerUploadLatency_ms"

	/*
		Time since the worker started
	*/
	WorkerUptimeGauge_ms = "workerUptimeGauge_ms"

	/*
		The amount of time a worker node was idle between tasks
	*/
	WorkerIdleLatency_ms = "workerIdleLatency_ms"

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
		The number of times a gitdb stream backend had to resort to a git fetch
	*/
	GitStreamUpdateFetches = "gitStreamUpdateFetches"

	/****************************** Saga Metrics ****************************************/
	/*
		The amount of time spent in looping through the buffered update channel to accumulate
		the updates in a batch, to be processed together
	*/
	SagaUpdateStateLoopLatency_ms = "sagaUpdateStateLoopLatency_ms"

	/*
		The amount of time spent in (bulk) updating the saga state and storing the messages in sagalog
	*/
	SagaUpdateStateLatency_ms = "sagaUpdateStateLatency_ms"

	/*
		The number of updates that were processed together by the updateSagaState loop
	*/
	SagaNumUpdatesProcessed = "sagaNumUpdatesProcessed"

	/*
		The amount of time spent in updating the saga state and sagalog when a task starts or ends
	*/
	SagaStartOrEndTaskLatency_ms = "sagaStartOrEndTaskLatency_ms"
)
