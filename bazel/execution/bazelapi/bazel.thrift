# Structures for storing Bazel request fields in tasks started from
# Bazel Execute Requests and job statuses (Action Results) polled by
# Bazel Get Operation requests

# See github.com/twitter/scoot/README.md for local Thrift prerequisites
# 
# To Generate files, run from top level (github.com/twitter/scoot) repo directory:
#     $ make thrift-bazel-go

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Digest
struct Digest {
  1: required string hash
  2: required i64 sizeBytes
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Platform_Property
struct Property {
  1: required string name
  2: required string value
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Action
struct Action {
  1: required Digest commandDigest
  2: required Digest inputDigest
  3: optional list<string> outputFiles
  4: optional list<string> outputDirs
  5: optional list<Property> platformProperties
  6: optional i64 timeoutMs
  7: optional bool noCache
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ExecuteRequest
struct ExecuteRequest {
  1: required Action action
  2: optional string instanceName
  3: optional bool skipCache
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#OutputFile
struct OutputFile {
  1: required Digest digest
  2: optional string path
  3: optional binary content
  4: optional bool isExecutable
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#OutputDirectory
struct OutputDirectory {
  1: required Digest treeDigest
  2: optional string path
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ActionResult
struct ActionResult {
  1: required Digest stdoutDigest
  2: required Digest stderrDigest
  3: optional binary stdoutRaw
  4: optional binary stderrRaw
  5: optional list<OutputFile> outputFiles
  6: optional list<OutputDirectory> outputDirectories
  7: optional i32 exitCode
}
