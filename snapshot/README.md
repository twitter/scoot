# Snapshots

Snapshots are Scoot's representation of file system states. Snapshots are immutable,
and are labelled by a unique ID.

The snapshot code consists of the main Snapshot interface, and various implementations
for dealing with snapshots in different filesystem paradigms - simple directories, git
repositories, etc.

Other key concepts here include:
* Filer
* Checkout / Checkouter
* Repo and RepoPool
