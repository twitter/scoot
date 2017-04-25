exception InvalidRequest {
  1: optional string message
}

exception CanNotScheduleNow {
  1: optional i64 retryAfterMs
}

# Generic Scoot Server Error equivalent to 500
exception ScootServerError {
  1: optional i64 retryAfterMs
}

enum JobType {
  UNKNOWN=1,
  IRON_TESTS=2,
}

enum RunStatusState {
  UNKNOWN = 0      # Reserved.
  PENDING = 1      # Run scheduled but not yet started.
  RUNNING = 2      # Run is happening.
  COMPLETE = 3     # Succeeded or failed yielding an exit code. Only state with an exit code.
  FAILED = 4       # Run mechanism failed and run is no longer active. Retry may or may not work.
  ABORTED = 5      # User requested that the run be killed.
  TIMEDOUT = 6     # Run timed out and was killed.
  BADREQUEST = 7   # Invalid or error'd request. Original worker state not affected. Retry may work after mutation.
}

// Note, each worker has its own runId space which is unrelated to any external ids.
struct RunStatus {
  1: required RunStatusState status
  2: required string runId
  3: optional string outUri
  4: optional string errUri
  5: optional string error
  6: optional i32 exitCode
  7: optional string snapshotId
  8: required string clientId
}


struct Command {
  1: list<string> argv
}

struct TaskDefinition {
  1: required Command command,
  2: optional string snapshotId,
  3: required string clientId,
}

struct JobDefinition {
  1: required map<string, TaskDefinition> tasks,
  2: optional JobType jobType,
}

struct JobId {
  1: required string id
}

enum Status {
  # NotRunning, waiting to be scheduled
  NOT_STARTED=1,

  # Currently Scheduled and In Progress Job/Task
  IN_PROGRESS=2,

  # Successfully Completed Job/Task
  COMPLETED=3,

  # Job was Aborted, Compensating Tasks are being Applied.
  # A RollingBack task has not finished its compensating
  # tasks yet.
  ROLLING_BACK=4,

  # Job/Task finished unsuccessfully all compensating actions
  # have been applied.
  ROLLED_BACK=5,
}

struct JobStatus {
  1: required string id,
  2: required Status status,
  3: optional map<string, Status> taskStatus,
  4: optional map<string, RunStatus> taskData,
}

service CloudScoot {
   JobId RunJob(1: JobDefinition job) throws (
    1: InvalidRequest ir,
    2: CanNotScheduleNow cnsn,
  )
  JobStatus GetStatus(1: string jobId) throws (
    1: InvalidRequest ir,
    2: ScootServerError err,
  )
}
