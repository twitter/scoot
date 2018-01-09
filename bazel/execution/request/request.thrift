# Structures for storing Bazel request fields in
# tasks started from Bazel Execute Requests

# See github.com/twitter/scoot/README.md for local Thrift prerequisites
# 
# To Generate files, run from this (github.com/twitter/scoot/bazel/execution/request/) directory:
#     1. thrift --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/request/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift request.thrift

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Digest
struct BazelDigest {
  1: required string hash
  2: required i64 sizeBytes
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Platform_Property
struct BazelProperty {
  1: required string name
  2: required string value
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#OutputFile
struct BazelOutputFile {
  1: optional string path
  2: required BazelDigest digest
  3: optional binary content
  4: optional bool isExecutable
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#OutputDirectory
struct BazelOutputDirectory {
  1: optional string path
  2: required BazelDigest digest
  3: required BazelDigest treeDigest
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#Action
struct BazelAction {
  1: required BazelDigest commandDigest
  2: required BazelDigest inputDigest
  3: optional list<string> outputFiles
  4: optional list<string> outputDirs
  5: optional list<BazelProperty> platformProperties
  6: optional i64 timeoutMs
  7: optional bool noCache
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ExecuteRequest
struct BazelExecuteRequest {
  1: required BazelAction action
  2: optional string instanceName
  3: optional bool skipCache
}

# Modeled after https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ActionResult
struct BazelActionResult {
  1: optional list<BazelOutputFile> outputFiles
  2: optional list<BazelOutputDirectory> outputDirectories
  3: optional i32 exitCode
  4: optional binary stdoutRaw
  5: required BazelDigest stdoutDigest
  6: optional binary stderrRaw
  7: required BazelDigest stderrDigest
}
