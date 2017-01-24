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
For now names look like '%s-%s-%s', where the third string can include additional dashes.
A path may be appended to this name and is recognized to start at the first occurence of '/'.
This path is only allowed when reading a bundle, otherwise it's considered an error if included.

The '/' is not allowed in bundle names except at the end where it signifies the start
of a path to be extracted from the bundle data. For now that bundle data must have been
created with a nil basis otherwise extraction will fail.

## Server
Server makes a store accessible via http and doesn't do much else at this time. Future work
includes ex: bundle validation, and generating better bundles with a different basis.

The use case for server is motivated by snapshot/git/gitdb/*. which needs to upload/download
bundles from persistent storage and does so by contacting this [off-box] server via httpStore.
Note that the server in turn may use httpStore internally to talk to, for instance, a SAN.
