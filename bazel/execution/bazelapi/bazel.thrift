# Structures for storing Bazel request fields in tasks started from
# Bazel Execute Requests and job statuses (Action Results) polled by
# Bazel Get Operation requests

# See github.com/twitter/scoot/README.md for local Thrift prerequisites
# 
# To Generate files, run from top level (github.com/twitter/scoot) repo directory:
#     $ make thrift-bazel-go

# NOTE on Thrift IDL - Always define included data structures above the structures
# that use them, as Thrift will generate undesirable code otherwise

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Digest
struct Digest {
  1: optional string hash
  2: optional i64 sizeBytes
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Platform_Property
struct Property {
  1: optional string name
  2: optional string value
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Action
struct Action {
  1: optional Digest commandDigest
  2: optional Digest inputDigest
  3: optional list<string> outputFiles
  4: optional list<string> outputDirs
  5: optional list<Property> platformProperties
  6: optional i64 timeoutMs
  7: optional bool noCache
}

# Modeled after https://godoc.org/github.com/golang/protobuf/ptypes/timestamp
struct Timestamp {
    1: optional i64 seconds
    2: optional i32 nanos
}

# Prototype, modeled after bazel/remoteexecution/remote_execution.proto#ExecutedActionMetadata
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

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ExecuteRequest
# Added Digest field for passing around actionDigest
# Added ExecutionMetadata field so worker has access to scheduling timestamp data
struct ExecuteRequest {
  1: optional Action action
  2: optional string instanceName
  3: optional bool skipCache
  4: optional Digest actionDigest
  5: optional ExecutedActionMetadata executionMetadata
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#OutputFile
struct OutputFile {
  1: optional Digest digest
  2: optional string path
  3: optional binary content
  4: optional bool isExecutable
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#OutputDirectory
struct OutputDirectory {
  1: optional Digest treeDigest
  2: optional string path
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ActionResult
# Added Digest field for passing around actionDigest
# Added GRPCStatus field for passing a googleapis rpc status value as protobuf-serialized bytes
# Added cached field for signaling whether the result was retrieved from ActionCache and not executed
struct ActionResult {
  1: optional Digest stdoutDigest
  2: optional Digest stderrDigest
  3: optional binary stdoutRaw
  4: optional binary stderrRaw
  5: optional list<OutputFile> outputFiles
  6: optional list<OutputDirectory> outputDirectories
  7: optional i32 exitCode
  8: optional Digest actionDigest
  9: optional binary GRPCStatus
  10: optional bool cached
  11: optional ExecutedActionMetadata executionMetadata
}
