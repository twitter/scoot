### Binaries

A location for site-specific binaries that inject dependencies.

Scoot is built with many libraries which will get assembled into several binaries. These binaries should be common, but each Scoot site may want to link in custom code to integrate with their own infrastructure. This directory holds binaries that do the dependency injection before calling libraries. Copy these and modify them to include your own implementations of interfaces.

* __setup-cloud-scoot__ - sets up local Scoot components (scheduler and worker), or sets up connection to remote ones
* __scheduler__ - the Scoot scheduler
* __workserver__ - the Scoot worker
* __daemon__ - local process that can act as a worker or scheduler proxy
* __scootapi__ - CLI client for Cloud Scoot API (scheduler)
* __workercl__ - CLI client for workers
* __scootcl__ - CLI client for daemon
* __minfs__ - TODO
