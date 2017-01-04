/*
package snapshot offers access to Scoot data.
Scoot data is largely snapshots (hence the package name).

The main entry point is the interface DB. DB holds Values which can be a Snapshot or a SnapshotWithHistory.

A Snapshot is a snapshot of filesystem state (like a tar file).

A SnapshotWithHistory is a Snapshot and also a commit message and an optional list of parent SnapshotWithHistory's.
(This definition corresponds with a git commit, which is convenient for creating git repos.)

There may be more Value types in the future. E.g., a file (without the directory structure).
*/
package snapshot
