# GitDB

GitDB is an implementation of ScootDB (snapshot.DB) that stores local values in a git ODB.

## Code Structure Overview
* _db.go_ structure definition, top-level entry point, concurrency control
* _backends.go_ ID definition and parsing; backend definition
* _create.go_ create Snapshots (locally)
* _checkout.go_ run git commands to checkout
* _local_data.go_ Snapshots stored locally
* _stream.go_ get Snapshots from an upstream git repo

## Backends
GitDB uses different Backends to identify, upload and download Snapshots.

## Exit Codes
GitDB defines a set of exit codes that are returned depending on various git related errors (see common/errors/exit_codes.go for definitive list)

## Snapshot ID format
GitDB uses Snapshot IDs that encode the backend, kind and per-backend data, separated by '-':
* _<backend>-<kind>(-<additional information>)+_

E.g.:
* local-gc-3cf4f8cf84976621c3ca9f19dff114842b4e5db7
  * backend: local
  * kind: GitCommitSnapshot
  *sha: 3cf4f8cf84976621c3ca9f19dff114842b4e5db7
* stream-gc-sm-3cf4f8cf84976621c3ca9f19dff114842b4e5db7
  * backend: stream
  * kind: GitCommitSnapshot
  * stream name: source_master
  *sha: 3cf4f8cf84976621c3ca9f19dff114842b4e5db7
* bs-fs-sm-530c6daad567a0765c10064e1c7fc4fa486a2638-d1f58ef31066244fc8590bfb6940b4060b1baab1
  * backend: Bundlestore
  * kind: FSSnapshot
  * stream name: source_master
  * bundle name: 530c6daad567a0765c10064e1c7fc4fa486a2638
  * sha: d1f58ef31066244fc8590bfb6940b4060b1baab1
