//Questions:
//  Should the worker or the scheduler generate taskID?
//  Will we want to support batching of tasks?
//  Do we need a way to restart workers?
//  What else would be useful in WorkerStatus? Load info?

enum Status {
  UNKNOWN = 0
  READY = 1
  PENDING = 2
  RUNNING = 3
  COMPLETED = 4
  ABORTED = 5
  TIMEOUT = 6
}

struct WorkerStatus {
  1: required list<string> taskIds
}

struct TaskStatus {
  1: required Status status
  2: optional string taskId
  3: optional string outUri
  4: optional string errUri
  5: optional string info
  6: optional i32 exitCode
}

struct Task {
  1: required list<string> argv
  2: optional string taskId
  3: optional string snapshotId
  4: optional i32 timeoutMs
}

service Worker {
  WorkerStatus QueryWorker()
  TaskStatus RunTask(1: Task task)
  TaskStatus QueryTask(1: string taskId)
  TaskStatus AbortTask(1: string taskId)
}
