# Scoot Setup

This package Cloud Scoot Setup, which is a mechanism for
running Scoot with local components, or initializing a connection
to remote ones, depending on configuration.

* __worker__ - interface for scheduler to start tasks on a worker & manage responses re: status of in progress tasks.
    * __workers__ - implementations of the worker interface (polling worker, etc), that invoke a runner