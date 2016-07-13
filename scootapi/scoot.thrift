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

service CloudScoot {
   JobId RunJob(1: JobDefinition job) throws (
    1: InvalidRequest ir,
    2: CanNotScheduleNow cnsn,
  )
}
