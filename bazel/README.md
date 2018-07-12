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
Testing the gRPC APIs can be done with the grpc_cli tool, but formatting the proper data structures is difficult,
so this method is unsupported. This has to be built and installed from the grpc repo. See:
https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md

#### Twitter MacBook installation pointers
In general, the grpc install instructions are accurate, but dependency installation via brew might not work.
Use a global, non-MDE copy of brew to install dependencies such as gflags, as MDE brew install will not make
these libraries accessible when trying to compile and link the grpc binaries.

### Usage Example

#### Remote Execution End-to-End Test Example with BZUtil
This details how to run a Bazel Remote Execution request in Scoot from scratch.
The worflow will be:
1. Start up a scheduler, apiserver, and workerserver
2. Upload the request Command (argv and env) to the apiserver via the CAS API
3. Upload the request input directory to the apiserver via fs_util/CAS API
4. Upload the request Action (references Command and input) to the apiserver via the CAS API
5. Schedule the request on the scheduler via the Execution API
6. Get results of the request from the scheduler via the Longrunning API

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
Example uploading contents of ~/workspace/bazel/dir/. Note that ~/workspace/bazel/db/ must exist, but can be empty.
We don't need this dir for our particular command, but we're uploading one as an example anyway:
```sh
fs_util --local-store-path=/Users/$USER/workspace/bazel/db --server-address=localhost:12100 directory save --root /Users/$USER/workspace/bazel/dir "**"
89a9068bd2d2784d5379b9fa3d02f02d9d0d7ecf4998a8e45b3d6784aacad4d4 157
```

4:
```sh
bzutil upload_action --cas_addr=localhost:12100 --command="1833d7c57656d2b7ee97e2068ce742f80e61357fba12a8b8d627782da3a58c29/11" --input_root="89a9068bd2d2784d5379b9fa3d02f02d9d0d7ecf4998a8e45b3d6784aacad4d4/157"
INFO[0000] Wrote to CAS successfully                     sourceLine="bzutil/main.go:220"
0cb08d1ec25eeed4d7a10d8a2cce85ea7810b76cfd43926e305288b19cf9aa3c/141
```

5:
```sh
bzutil execute --action="0cb08d1ec25eeed4d7a10d8a2cce85ea7810b76cfd43926e305288b19cf9aa3c/141"
INFO[0000] Operation: b53b5e99-0162-43e1-6cbd-e44f8e6d1566
	Done: false
	Metadata:
		Stage: QUEUED
		ActionDigest: 905f62f396fd837d111ac96007e38502736799a98f7207f90460004667e7fe25/138
  sourceLine="bzutil/main.go:236"
```

6:
```sh
bzutil get_operation --name="b53b5e99-0162-43e1-6cbd-e44f8e6d1566"
INFO[0000] Operation: b53b5e99-0162-43e1-6cbd-e44f8e6d1566
	Done: true
	Metadata:
		Stage: COMPLETED
		ActionDigest: 0cb08d1ec25eeed4d7a10d8a2cce85ea7810b76cfd43926e305288b19cf9aa3c/141
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
			ExecutionMetadata:
				Worker: bazel-worker
				QueueLatency: 420ms
				WorkerTotal: 17078ms
				InputFetch: 35ms
				Execution: 17031ms
				OutputUpload: 3ms
  sourceLine="bzutil/main.go:246"
```

### GRPC through a proxy
If your GRPC-serving binaries are only accessible via a proxy, it is possible to redirect GRPC client commands via:

```sh
ssh <proxy-host> -L <local-proxy-port>:<service-host>:<service-port> -N &
grpc_cli call localhost:<local-proxy-port> ...
```
