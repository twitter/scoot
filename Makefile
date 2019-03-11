NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/twitter/scoot"
FIRSTGOPATH := $(shell echo $${GOPATH%%:*})

SHELL := /bin/bash -o pipefail

# Libaries don't configure the logger by default - define this so they can init the logger during testing.
SCOOT_LOGLEVEL ?= info

# Output can be overly long and exceed TravisCI 4MB limit, so filter out some of the noisier logs.
# Hacky redirect interactive console to 'tee /dev/null' so logrus on travis will produce full timestamps.
TRAVIS_FILTER ?= 2>&1 | tee /dev/null | egrep -v 'line="(runners|scheduler/task_|gitdb)'

default:
	go build $$(go list ./... | grep -v /vendor/)

dependencies:
	# Populates the vendor directory to reflect the latest run of check-dependencies.

	# Checkout our vendored dependencies.
	# Note: The submodule dependencies must be initialized prior to running scoot binaries.
	#       When used as a library, the vendor folder will be empty by default (if 'go get'd).
	git submodule update --init --recursive

	# Install mockgen binary (it's only referenced for code gen, not imported directly.)
	# Both the binary and a mock checkout will be placed in $GOPATH (duplicating the vendor checkout.)
	# We use 'go get' here because 'go install' will not build out of our vendored mock repo.
	go get github.com/golang/mock/mockgen

	# Install go-bindata tool which is used to generate binary version of config file
	# this is only used by go generate
	go get github.com/twitter/go-bindata/...

check-dependencies:
	# Run this whenever a dependency is added.
	# We run our own script to get all transitive dependencies. See github.com/pantsbuild/pants/issues/3606.
	./deps.sh
	go get github.com/golang/mock/mockgen

generate:
	go generate $$(go list ./... | grep -v /vendor/)

format:
	go fmt $$(go list ./... | grep -v /vendor/)

fs_util:
	# Fetches fs_util tool from pantsbuild binaries
	sh get_fs_util.sh

vet:
	go vet $$(go list ./... | grep -v /vendor/)

coverage:
	sh testCoverage.sh $(TRAVIS_FILTER)

test-unit-property-integration: fs_util
	# Runs all tests including integration and property tests
	go test -count=1 -race -timeout 120s -tags="integration property_test" $$(go list ./... | grep -v /vendor/ | grep -v /cmd/) $(TRAVIS_FILTER)

test-unit-property:
	# Runs only unit tests and property tests
	go test -count=1 -race -timeout 120s -tags="property_test" $$(go list ./... | grep -v /vendor/ | grep -v /cmd/) $(TRAVIS_FILTER)

test-unit:
	# Runs only unit tests
	# Only invoked manually so we don't need to modify output
	go test -count=1 -race -timeout 120s $$(go list ./... | grep -v /vendor/ | grep -v /cmd/)

testlocal: generate test

test: test-unit-property-integration coverage

swarmtest:
	# Setup a local schedule against local workers (--strategy local.local)
	# Then run (with go run) scootapi run_smoke_test with 10 jobs, wait 1m
	# We build the binaries becuase 'go run' won't consistently pass signals to our program.
	go install ./binaries/...
	$(FIRSTGOPATH)/bin/setup-cloud-scoot --strategy local.local run scootapi run_smoke_test --num_jobs 10 --timeout 1m $(TRAVIS_FILTER)

recoverytest:
	# Some overlap with swarmtest but focuses on sagalog recovery vs worker/checkout correctness.
	# We build the binaries becuase 'go run' won't consistently pass signals to our program.
	# Ignore output here to reduce travis log size. Swarmtest is more important and that still logs.
	go install ./binaries/...
	$(FIRSTGOPATH)/bin/recoverytest &>/dev/null

integrationtest:
	# Integration test with some overlap with other standalone tests, but utilizes client binaries
	go install ./binaries/...
	$(FIRSTGOPATH)/bin/scoot-integration &>/dev/null
	$(FIRSTGOPATH)/bin/bazel-integration &>/dev/null

clean-mockgen:
	rm -rf */*_mock.go

clean-data:
	rm -rf ./.scootdata/*

clean-go:
	go clean ./...

clean: clean-data clean-mockgen clean-go

fullbuild: dependencies generate test

travis: dependencies fs_util recoverytest swarmtest integrationtest test clean-data

thrift-worker-go:
	# Create generated code in github.com/twitter/scoot/workerapi/gen-go/... from worker.thrift
	cd workerapi && thrift -I ../bazel/execution/bazelapi/ --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift worker.thrift && cd ..
	rm -rf workerapi/gen-go/worker/worker-remote/

thrift-sched-go:
	# Create generated code in github.com/twitter/scoot/sched/gen-go/... from sched.thrift
	cd sched && thrift -I ../bazel/execution/bazelapi/ --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift sched.thrift && cd ..

thrift-scoot-go:
	# Create generated code in github.com/twitter/scoot/scootapi/gen-go/... from scoot.thrift
	cd scootapi && thrift -I ../bazel/execution/bazelapi/ --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift && cd ..
	rm -rf scootapi/gen-go/scoot/cloud_scoot-remote/

thrift-bazel-go:
	# Create generated code in github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/... from bazel.thrift
	cd bazel/execution/bazelapi && thrift --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift bazel.thrift && cd ..

thrift-go: thrift-sched-go thrift-scoot-go thrift-worker-go thrift-bazel-go

thrift: thrift-go

bazel-proto:
	cp vendor/github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2/remote_execution.proto bazel/remoteexecution/
	protoc -I bazel/remoteexecution/ -I ~/workspace/src/github.com/googleapis/googleapis/ bazel/remoteexecution/remote_execution.proto --go_out=plugins=grpc:bazel/remoteexecution
