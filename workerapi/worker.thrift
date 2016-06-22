enum Status {
  UNKNOWN = 0
  READY = 1
  PENDING = 2
  FAILURE = 3
  RUNNING = 4
  SUCCESS = 5
  ABORTED = 6
  TIMEOUT = 7
}

struct WorkerStatus {
  1: required Status status
  2: optional string taskId
  3: optional string outUri
  4: optional string errUri
  5: optional string info
  6: optional i32 exitCode
}

struct Task {
  1: required list<string> argv
  2: optional list<string> env
  3: optional string dir
  4: optional string taskId
  5: optional string sourceId //Allows snapshotId or patchId
  6: optional i32 timeout_ms
}

service Worker {
  WorkerStatus QueryStatus()
  WorkerStatus Run(1: Task task)
  WorkerStatus Abort(
    1: string taskId
    2: bool force
  )
  // Do we need a func to restart workers?
}
