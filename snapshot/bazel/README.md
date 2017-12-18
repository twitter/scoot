# Bazel Remote Execution Snapshot

This contains snapshot support for the Bazel gRPC Remote Execution API
We currently rely on fs_util, a tool distributed by github.com/pantsbuild/pants, for underlying implementation

### Snapshot ID format
Bazel Remote Execution snapshots use Snapshot IDs of format bz-<sha256>-<sizeBytes> and map to a unique Bazel digest

### Components:
* bzFiler satisfies the snapshot.Snapshot interface
  * Checks out a Bazel Digest as a snapshot & sets up local filesystem
  * Ingests directory or file and stores it as a snapshot
  * noop Updates
* bzCheckout satisfies the snapshot.Checkout interface
* bzCommand runs fs_util tool as an exec.Cmd with relevant options / flags / args
  * Possible arguments and expected output
    * file save [path] -> <sha> <size>
    * directory save --root [root] \[dir] -> <sha> <size>
    * directory materialize <sha> <path>
