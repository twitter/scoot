service Proc {
  
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
    1: list<string> cmds
  }

  struct Task {
    1: optional string id
    2: required Command command,
    3: optional string snapshotId,
    4: optional string contextId
  }

  struct Job {
    1: required string id,
    2: required list<Task> tasks
  }

  Job RunJob(
    1: optional string contextId,
    2: optional JobType jobType,
    3: required list<Task> tasks
  ) throws (
    1: InvalidRequest ir,
    2: ScootTooBusy stb
  )
}
