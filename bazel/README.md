# Bazel Remote Execution for Scoot

This contains initial server support for the Bazel gRPC Remote Execution API.
This consists of two distinct concepts, the Execution API and ContentAddressableStore (CAS) API.

The Bazel API Design Document can be found at: https://docs.google.com/document/d/1AaGk7fOPByEvpAbqeXIyE8HX_A3_axxNnvroblTZ_6s/edit#

### Components:
* cas/ contains CAS API server implementation
* execution/ contains Execution API server implementation
* ./ (bazel) contains general Bazel constants, utils, and a gRPC server abstraction

### Running/testing the API:
* Scheduler will initialize and serve the Execution API over gRPC on the default port.
* Apiserver will initialize and serve the CAS API over gRPC on the default port.

```sh
go install github.com/twitter/scoot/binaries/scheduler github.com/twitter/scoot/binaries/apiserver
./scheduler
./apiserver
```

## GRPC CLI Client
Testing the gRPC APIs can be done with the grpc_cli tool, which can be used as a client for any gRPC server.
This has to be built and installed from the grpc repo. See:
https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md

#### Twitter MacBook installation pointers
In general, the grpc install instructions are accurate, but dependency installation via brew might not work.
Use a global, non-MDE copy of brew to install dependencies such as gflags, as MDE brew install will not make
these libraries accessible when trying to compile and link the grpc binaries.

### Usage Example
#### CAS - Apiserver
This is a brief example of using a locally running default apiserver for a CAS Write and Read request using grpc_cli:

```sh
apiserver
```

```sh
[...]$ grpc_cli call localhost:9098 google.bytestream.ByteStream.Write "resource_name: 'uploads/123e4567-e89b-12d3-a456-426655440000/blobs/ce58a4479be1d32816ee82e57eae04415dc2bda173fa7b0f11d18aa67856f242/7', write_offset: 0, finish_write: true, data: 'abc1234'"
reading streaming request message from stdin...
Request sent.
got response.
committed_size: 7

^C
```

```sh
[...]$ grpc_cli call localhost:9098 google.bytestream.ByteStream.Read "resource_name: 'blobs/ce58a4479be1d32816ee82e57eae04415dc2bda173fa7b0f11d18aa67856f242/7', read_offset: 0, read_limit: 0"
connecting to localhost:9098
data: "abc1234"

Rpc succeeded with OK status
```

#### Execution - Scheduler
This is a brief example of using a locally running default scheduler for an Execute request using grpc_cli:

```sh
scheduler
```

```sh
[...]$ grpc_cli call localhost:9099 google.devtools.remoteexecution.v1test.Execution.Execute "action: {command_digest: {hash: 'abc123', size_bytes: 0}, input_root_digest: {hash: 'def456', size_bytes: 0}}"
connecting to localhost:9099
name: "operations/737a1171-dea7-47c6-4585-f3f7d4f0245e"
metadata {
  type_url: "type.googleapis.com/google.devtools.remoteexecution.v1test.ExecuteOperationMetadata"
  value: "\010\004\022D\n@522578c80d054569075825ebc82573d7d9c429178d3ecf7a9e276b115fa7837f\020\024"
}
done: true
response {
  type_url: "type.googleapis.com/google.devtools.remoteexecution.v1test.ExecuteResponse"
  value: "\n\000"
}

Rpc succeeded with OK status
```
