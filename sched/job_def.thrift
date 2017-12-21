# Structures for storing the internal JobDefinition.
# We use this structure rather than scootapi's JobDefinition (in scoot.thrift)
# so that we can add data to the log and not impact the client API.

# We should use go generate to run:
# For now to Install Thrift:
#     1. Install Thrift manually `brew install thrift` ensure version is greater that 0.9.3
#     2. go get github.com/apache/thrift/lib/go/thrift
#

# To Generate files run from this (github.com/twitter/scoot/sched) directory
#     1. thrift --gen go:package_prefix=github.com/twitter/scoot/sched/gen-go/,package=schedthrift,thrift_import=github.com/apache/thrift/lib/go/thrift job_def.thrift

struct Command {
  1: required list<string> argv
  2: optional map<string, string> envVars
  3: optional i64 timeout
  4: required string snapshotId
}

struct TaskDefinition {
  1: required Command command
  2: optional string taskId
  3: optional BazelExecuteRequest bazelRequest
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

# Structures for storing Bazel request fields in
# Tasks started from Bazel Execute Requests

struct BazelDigest {
  1: required string hash
  2: required i64 sizeBytes
}

struct BazelProperty {
  1: required string name
  2: required string value
}

struct BazelAction {
  1: required BazelDigest commandDigest
  2: required BazelDigest inputDigest
  3: optional list<string> outputFiles
  4: optional list<string> outputDirs
  5: optional list<BazelProperty> platformProperties
  6: optional i64 timeoutMs
  7: optional bool noCache
}

struct BazelExecuteRequest {
  1: required BazelAction action
  2: optional string instanceName
  3: optional bool skipCache
}
