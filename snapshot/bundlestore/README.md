# Bundlestore

## Overview
Stores are the way we persist and retrieve bundles.
They deal with the moving of bundle data, not its generation in any way.
The storage may be local or remote, and for simplicity we don't expose that detail.
Functions may perform special processing and caching that differ from store to store.

There are a few different store types that handle bundles differently. They can use bundles
in memory, bundles on disk, and bundles located behind an http server. Further, we can
wrap these stores in a parent store to add additional business logic to our handling.

## Bundle name conventions
* HTTP Bundlestore server - For now names look like 'bs-<sha>.bundle'

## Server
Server makes a store accessible via http and doesn't do much else at this time. Future work
includes ex: bundle validation, and generating better bundles with a different basis.

The use case for server is motivated by snapshot/git/gitdb. which needs to upload/download
bundles from persistent storage and does so by contacting this [off-box] server via httpStore.
Note that the server in turn may use httpStore internally to talk to, for instance, a SAN.

### Server API

#### GET
Example:
```sh
curl -X GET -o local-output.bundle http://localhost:9094/bundle/bs-0000000000000000000000000000000000000000.bundle
```

#### POST
Example:
```sh
curl -X POST --data-binary "@/abspath/local-input.bundle" http://localhost:9094/bundle/bs-0000000000000000000000000000000000000000.bundle
```
