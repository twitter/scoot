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

struct Command {
  1: list<string> argv
}

struct TaskDefinition {
  1: required Command command,
  2: optional string snapshotId,
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

