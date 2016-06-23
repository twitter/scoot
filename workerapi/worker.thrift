enum Status {
  UNKNOWN = 0
  READY = 1
  PENDING = 2
  RUNNING = 3
  COMPLETED = 4
  ABORTED = 5
  TIMEOUT = 6
}

//TODO: add useful load information when it comes time to have multiple runs.
struct WorkerStatus {
  1: required list<string> running
}

struct RunStatus {
  1: required Status status
  2: optional string runId
  3: optional string outUri
  4: optional string errUri
  5: optional string info
  6: optional i32 exitCode
}

struct RunCommand {
  1: required list<string> argv
  2: optional string snapshotId
  3: optional i32 timeoutMs
}

//TODO: add a method to kill the worker if we can articulate unrecoverable issues.
service Worker {
  WorkerStatus QueryWorker()
  RunStatus Run(1: RunCommand cmd)
  RunStatus Query(1: string runId)
  RunStatus Abort(1: string runId)
}
