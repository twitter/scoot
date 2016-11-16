# Scoot API

Scoot API contains the Scoot Cloud API Thrift definition, Cloud Server code
and client/CLI code for the API.

__Dependencies for installing thrift__
* Install Thrift manually (version >= 0.9.3). macOS:
```sh
brew install thrift
```

* Thrift for go:
```sh
go get github.com/apache/thrift/lib/go/thrift
```

__Generating thrift files from this diectory__
* To Generate files run from this directory:
```sh
thrift --gen go:package_prefix=github.com/scootdev/scoot/scootapi/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift
```
