### Binaries

A location for site-specific binaries that inject dependencies.

Scoot is built with many libraries which will get assembled into several binaries. These binaries should be common, but each Scoot site may want to link in custom code to integrate with their own infrastructure. This directory holds binaries that do the dependency injection before calling libraries. Copy these and modify them to include your own implementations of interfaces.

* __bazel-integration__ - an integration test for scoot's grpc endpoints
* __bzutil__ - CLI client for scoot's grpc endpoints
* __recoverytest__ - an integration test for sagalog recovery
* __scoot-integration__ - an integration test for scoot's thrift endpoints
* __scoot-snapshot-db__ - CLI client for snapshot store (via apiserver)
* __workercl__ - CLI client for workers
* __workserver__ - the Scoot worker service
