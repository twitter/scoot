# Structures for storing the internal JobDefinition.
# We use this structure rather than the scoot API's JobDefinition (in scoot.thrift)
# so that we can add data to the log and not impact the client API.

# See github.com/twitter/scoot/README.md for local Thrift prerequisites
#
# To Generate files, run from top level (github.com/twitter/scoot) repo directory:
#     $ make thrift-sched-go

struct Command {
  1: required list<string> argv
  2: optional map<string, string> envVars
  3: optional i64 timeout
  4: required string snapshotId
}

struct TaskDefinition {
  1: required Command command
  2: optional string taskId
}

struct JobDefinition {
  1: optional string jobType,
  2: optional list<TaskDefinition> tasks,
  3: optional i32 priority
  4: optional string tag
  5: optional string basis
  6: optional string requestor
}

struct Job {
  1: required string id
  2: required JobDefinition jobDefinition
}
