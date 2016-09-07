# Structures for storing the internal JobDefinition.
# We use this structure rather than scootapi's JobDefinition (in scoot.thrift)
# so that we can add data to the log and not impact the client API.

# We should use go generate to run:
# For now to Install Thrift:
#     1. Install Thrift manually `brew install thrift` ensure version is greater that 0.9.3
#     2. go get github.com/apache/thrift/lib/go/thrift
#

# To Generate files run from this (github.com/scootdev/scoot/sched) directory
#     1. thrift --gen go:package_prefix=github.com/scootdev/scoot/sched/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift sagaLog.thrift


struct Command {
  1: list<string> argv,
  2: map<string, string> envVars,
  3: i64 timeout,
  4: string snapshotId,
}

struct TaskDefinition {
  1: Command command,
}

struct JobDefinition {
  1: string jobType,
  2: map<string, TaskDefinition> tasks,
}

struct Job {
  1: string id
  2: JobDefinition jobDefinition
}