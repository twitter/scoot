# Structures for storing Bazel request fields in tasks started from
# Bazel Execute Requests and job statuses (Action Results) polled by
# Bazel Get Operation requests

# See github.com/twitter/scoot/README.md for local Thrift prerequisites
# 
# To Generate files, run from top level (github.com/twitter/scoot) repo directory:
#     $ make thrift-bazel-go

# NOTE on Thrift IDL - Always define included data structures above the structures
# that use them, as Thrift will generate undesirable code otherwise

# Modeled after github.com/bazelbuild/remote-apis#Digest
struct Digest {
  1: optional string hash
  2: optional i64 sizeBytes
}

# Modeled after https://godoc.org/github.com/golang/protobuf/ptypes/timestamp
struct Timestamp {
    1: optional i64 seconds
    2: optional i32 nanos
}

# Modeled after github.com/bazelbuild/remote-apis#ExecutedActionMetadata
struct ExecutedActionMetadata {
    1: optional string worker
    2: optional Timestamp queuedTimestamp
    3: optional Timestamp workerStartTimestamp
    4: optional Timestamp workerCompletedTimestamp
    5: optional Timestamp inputFetchStartTimestamp
    6: optional Timestamp inputFetchCompletedTimestamp
    7: optional Timestamp executionStartTimestamp
    8: optional Timestamp executionCompletedTimestamp
    9: optional Timestamp outputUploadStartTimestamp
    10: optional Timestamp outputUploadCompletedTimestamp
}

# Modeled after github.com/bazelbuild/remote-apis#ExecutionPolicy
struct ExecutionPolicy {
  1: optional i32 priority
}

# Modeled after github.com/bazelbuild/remote-apis#ResultsCachePolicy
struct ResultsCachePolicy {
  1: optional i32 priority
}

# Modeled after github.com/bazelbuild/remote-apis#ExecuteRequest
# Added ExecutionMetadata field so worker has access to scheduling timestamp data
struct ExecuteRequest {
  1: optional string instanceName
  2: optional bool skipCache
  3: optional Digest actionDigest
  4: optional ExecutionPolicy executionPolicy
  5: optional ResultsCachePolicy resultsCachePolicy
  6: optional ExecutedActionMetadata executionMetadata
}

# Modeled after github.com/bazelbuild/remote-apis#OutputFile
struct OutputFile {
  1: optional string path
  2: optional Digest digest
  3: optional bool isExecutable
}

# Modeled after github.com/bazelbuild/remote-apis#OutputDirectory
struct OutputDirectory {
  1: optional string path
  2: optional Digest treeDigest
}

# Modeled after github.com/bazelbuild/remote-apis#ActionResult
# Added Digest field for passing around actionDigest
# Added GRPCStatus field for passing a googleapis rpc status value as protobuf-serialized bytes
# Added cached field for signaling whether the result was retrieved from ActionCache and not executed
struct ActionResult {
  1: optional list<OutputFile> outputFiles
  2: optional list<OutputDirectory> outputDirectories
  3: optional i32 exitCode
  4: optional binary stdoutRaw
  5: optional Digest stdoutDigest
  6: optional binary stderrRaw
  7: optional Digest stderrDigest
  8: optional ExecutedActionMetadata executionMetadata
  9: optional Digest actionDigest
  10: optional binary GRPCStatus
  11: optional bool cached
}
