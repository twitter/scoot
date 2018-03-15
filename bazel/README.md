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

## BZUtil CLI Client
The preferred client for Scoot operations is binaries/bzutil/main.go, which implements GRPC client interfaces.
For more raw testing of service interfaces, the generic grpc_cli client can be used.

## FS_UTIL Client
We leverage the fs_util client binary which is managed by the https://github.com/pantsbuild/pants/ project. We
coordinate to find suitable releases and specify them in the `get_fs_util.sh` script. The project Makefile will
fetch the correct client and make it available where needed by Scoot.
Binaries are fetched from https://binaries.pantsbuild.org

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

#### Remote Execution End-to-End Test Example with BZUtil
This details how to run a Bazel Remote Execution request in Scoot from scratch.
The worflow will be:
1. Start up a scheduler, apiserver, and workerserver
2. Upload the request Command (argv and env) to the apiserver via the CAS API
3. Upload the request input directory to the apiserver via fs_util/CAS API
4. Schedule the request on the scheduler via the Execution API
5. Get results of the request from the scheduler via the Longrunning API

1:
```sh
$GOPATH/bin/setup-cloud-scoot --strategy local.local
```

2:
```sh
bzutil upload_command --cas_addr="localhost:12100" sleep 17
INFO[0000] Using argv: ["sleep" "17"] env: map[]         sourceLine="bzutil/main.go:112"
INFO[0000] Wrote to CAS successfully                     sourceLine="bzutil/main.go:142"
1833d7c57656d2b7ee97e2068ce742f80e61357fba12a8b8d627782da3a58c29/11
```

3:
Example uploading contents of ~/workspace/dir/. Note that ~/workspace/db/ must exist, but can be empty.
We don't need this dir for our particular command, but we're uploading one as an example anyway:
```sh
fs_util --local-store-path=/Users/$USER/workspace/db --server-address=localhost:12100 directory save --root /Users/$USER/workspace/dir "**"
6740a42ee34b6f2f55af17cc01591042c7ccd69082228cf6838882fb046b02a8 76
```

4:
```sh
bzutil execute --command="1833d7c57656d2b7ee97e2068ce742f80e61357fba12a8b8d627782da3a58c29/11" --input_root="6740a42ee34b6f2f55af17cc01591042c7ccd69082228cf6838882fb046b02a8/76"
INFO[0000] Operation: dc42f75f-0fc8-4e6c-790f-edac95e6d8cf
	Done: false
	Metadata:
		Stage: QUEUED
		ActionDigest: 953ac960469b1a04ab7114d615aac7a804c435850cd527091bc85cf7114d071f/144
  sourceLine="bzutil/main.go:177"
```

5:
```sh
bzutil get_operation --name="dc42f75f-0fc8-4e6c-790f-edac95e6d8cf"
INFO[0000] Operation: dc42f75f-0fc8-4e6c-790f-edac95e6d8cf
	Done: true
	Metadata:
		Stage: COMPLETED
		ActionDigest: 953ac960469b1a04ab7114d615aac7a804c435850cd527091bc85cf7114d071f/144
	ExecResponse:
		Status:
			Code: OK
		Cached: false
		ActionResult:
			ExitCode: 0
			OutputFiles: []
			OutputDirectories: []
			StdoutDigest: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0
			StderrDigest: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0
  sourceLine="bzutil/main.go:187"
```

### GRPC through a proxy
If your GRPC-serving binaries are only accessible via a proxy, it is possible to redirect GRPC client commands via:

```sh
ssh <proxy-host> -L <local-proxy-port>:<service-host>:<service-port> -N &
grpc_cli call localhost:<local-proxt-port> ...
```
