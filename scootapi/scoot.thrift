exception InvalidRequest {
  1: optional string message
}

exception CanNotScheduleNow {
  1: optional i64 retryAfterMs
}

enum JobType {
  UNKNOWN=1,
  IRON_TESTS=2,
}

struct Command {
  1: list<string> argv
}

struct Task {
  1: optional string id
  2: required Command command,
  3: optional string snapshotId,
}

struct JobId {
  1: required string id
}

struct Job {
  1: required string id,
  2: required list<Task> tasks,
}

struct JobDefinition {
  1: required list<Task> tasks,
  2: optional JobType jobType,
}

// TODO(dbentley): rename this to... Exec? CloudExec? CloudScoot?
service Proc {
   JobId RunJob(1: JobDefinition job) throws (
    1: InvalidRequest ir,
    2: CanNotScheduleNow cnsn,
  )
}
