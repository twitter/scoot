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
  * _Filer_ - interface for treating snapshots as files, includes Checkouter and Ingester
  * _Checkout_ - Specific instance or "checkout" of a snapshot
  * _Checkouter_ - Wrapping interface for managing snapshots on the local filesystem
  * _Ingester_ - interface for creating snapshots from the local filesystem
* __git specific objects__
  * _package gitfiler_ - for checking out and handling snapshots from Git
    * _RepoPool_ - for handling concurrent access to Git repos
    * _RepoIniter_ - interface for controlling (possibly expensive) Git repo initialization
    * _Checkouter_ - a Git-specific snapshot checkouter implementation