/*
package snapshot offers access to Scoot data.
Scoot data is largely snapshots (hence the package name).

The main entry point is the interface DB. DB holds Snapshots which can be an FSSnapshot or a GitCommitSnapshot.

An FSSnapshot is a snapshot of filesystem state (like a tar file).

A GitCommitSnapshot mirrors a git commit: a Snapshot, commit metadata, and an optional list of parent GitCommitSnapshots.

There may be more Snapshot kinds in the future. E.g., a file (without the directory structure).
*/
package snapshot
