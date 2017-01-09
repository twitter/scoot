# GitDB

GitDB is an implementation of ScootDB (snapshot.DB) that stores local values in a git ODB.

## Code Structure Overview
* _db.go_ structure definition, top-level entry point, concurrency control
* _schemes.go_ ID definition and parsing
* _checkout.go_ run git commands to checkout
* _local_data.go_ Values creates locally
* _stream.go_ get Values from an upstream git repo
* _bundlestore.go_ upload Values to and download Values from Bundlestore

## Schemes
GitDB uses different Schemes to identify, upload and download Values depending on their provenance. (Like how Chrome treats http: URLs different from file: URLs.) Local storage is the same among Kinds (an object in the Git ODB), but download and upload are different. Schemes are orthogonal to Value Kinds (like how html vs. jpg is orthogonal to http: vs. file:). GitDB encodes the Scheme and other information in the Value IDs.

## Value ID format
GitDB uses Value IDs that encode the scheme, kind and per-scheme data, separated by '-':
* _<scheme>-<kind>(-<additional information>)+_

E.g.:
* local-swh-3cf4f8cf84976621c3ca9f19dff114842b4e5db7
  * scheme: local
  * kind: SnapshotWithHistory
  *sha: 3cf4f8cf84976621c3ca9f19dff114842b4e5db7
* stream-swh-sm-3cf4f8cf84976621c3ca9f19dff114842b4e5db7
  * scheme: stream
  * kind: SnapshotWithHistory
  * stream name: source_master
  *sha: 3cf4f8cf84976621c3ca9f19dff114842b4e5db7
* bs-s-sm-530c6daad567a0765c10064e1c7fc4fa486a2638-d1f58ef31066244fc8590bfb6940b4060b1baab1
  * scheme: Bundlestore
  * kind: Snapshot
  * stream name: source_master
  * bundle name: 530c6daad567a0765c10064e1c7fc4fa486a2638
  * sha: d1f58ef31066244fc8590bfb6940b4060b1baab1
