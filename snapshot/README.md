# Snapshots

Snapshots are Scoot's representation of file system states. Snapshots are immutable,
and are labelled by a unique ID.

The snapshot code consists of the main Snapshot interface, and various implementations
for dealing with snapshots in different filesystem paradigms - simple directories, git
repositories, etc.

Key objects (not exhaustive, see code or godoc for more):
* __Snapshots and Files__
  * _Snapshot_ - the main interface for representing snapshots and the files in them
  * _File_ - interface representing a file open for reading
  * _FileCursor_ - interface for reading specific segments of files
* __Higher-level abstractions for filesystem data__ - implementations in snapshot/snaphots/
  * _DB_ - interface to deal with Snapshots. Should replace the types below.
  * _Filer_ - interface for treating snapshots as files, includes Checkouter, Ingester and Updater
  * _Checkout_ - Specific instance or "checkout" of a snapshot
  * _Checkouter_ - Wrapping interface for managing snapshots on the local filesystem
  * _Ingester_ - interface for creating snapshots from the local filesystem
  * _Updater_ - interface for updating underlying resource a Filer is utilizing
* __git specific objects__
    * _RepoPool_ - for handling concurrent access to Git repos
    * _RepoIniter_ - interface for controlling (possibly expensive) Git repo initialization
    * _Checkouter_ - a Git-specific snapshot checkouter implementation
  * _package gitdb_ - implementation of DB interface that stores local Snapshots in a git ODB

## Snapshot Stores and Servers

### Store
snapshot/store contains Snapshot storage interfaces and implementations.

### Bundlestore
snapshot/bundlestore contains a server for storing and retrieving Snapshots from a store.
