NAME := scoot
DESC := distributed build tools
GOVERSION := $(shell go version)
BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILDDATE := $(shell date -u +"%B %d, %Y")
PROJECT_URL := "https://github.com/twitter/scoot"
FIRSTGOPATH := $(shell echo $${GOPATH%%:*})
THRIFTVERSION := $(shell thrift -version | cut -d ' ' -f 3 | tr -d '\n')
PROTOCVERSION := $(shell protoc --version | cut -d ' ' -f 2 | tr -d '\n')
GO111MODULE := on
export GO111MODULE

SHELL := /bin/bash -o pipefail

# Libaries don't configure the logger by default - define this so they can init the logger during testing.
SCOOT_LOGLEVEL ?= info

# Output can be overly long and exceed TravisCI 4MB limit, so filter out some of the noisier logs.
# Hacky redirect interactive console to 'tee /dev/null' so logrus on travis will produce full timestamps.
TRAVIS_FILTER ?= 2>&1 | tee /dev/null | egrep -v 'line="(runners|scheduler/task_|gitdb)'

build:
	go build ./...

fmt:
	gofmt -l -w -s .

vet:
	go vet ./...

install:
	go install ./binaries/...
	go install ./perftests/...

# Cleans go.mod and go.sum of unused dependencies
tidy:
	go mod tidy

# Gets the version of REPO specified by VER
# Usage: make get REPO=cloud.google.com/go VER=v0.45.1
get:
	go get $(REPO)@$(VER)

# Gets the latest version of REPO, or if left blank, updates all modules,
# Usage: make get REPO=cloud.google.com/go
get_latest:
	go get -u $(REPO)

############## dependencies

# tool dependencies for developer workflows only (regenerating test mocks, bindata, thrift or proto code)
dev-dependencies:
	# Install mockgen binary (it's only referenced for code gen, not imported directly.)
	# Both the binary and a mock checkout will be placed in $GOPATH
	go get github.com/golang/mock/mockgen@58cd061d09382b6011f84c1291ebe50ef2e25bab

	# Install go-bindata tool which is used to generate binary version of config file
	# this is only used by go generate
	go get github.com/twitter/go-bindata@2fa2cba09795

	# check thrift, protoc versions
ifneq ($(THRIFTVERSION),0.9.3)
	echo "Wanted thrift version 0.9.3, got $(THRIFTVERSION)"
	exit 1
endif

ifneq ($(PROTOCVERSION),3.5.1)
	echo "Wanted protoc version 3.5.1, got $(PROTOCVERSION)"
	exit 1
endif

# universal dep needed to run scoot
fs_util:
	# Fetches fs_util tool from pantsbuild binaries
	bash get_fs_util.sh

############## tests and coverage

coverage:
	bash testCoverage.sh $(TRAVIS_FILTER)

# Usage: make test PKG=github.com/twitter/scoot/binaries/...
test:
	go test -count=1 -race -timeout 20s $(PKG)

test-unit-property-integration: fs_util
	# Runs all tests including integration and property tests
	go test -count=1 -race -timeout 120s -tags="integration property_test" $$(go list ./...) $(TRAVIS_FILTER)

test-unit-property:
	# Runs only unit tests and property tests
	go test -count=1 -race -timeout 120s -tags="property_test" $$(go list ./...) $(TRAVIS_FILTER)

test-unit:
	# Runs only unit tests
	# Only invoked manually so we don't need to modify output
	go test -count=1 -race -timeout 120s $$(go list ./...)

test-all: test-unit-property-integration coverage

############## standalone binary & integration tests

swarmtest: install
	# Setup a local schedule against local workers (--strategy local.local)
	# Then run (with go run) scootapi run_smoke_test with 10 jobs, wait 1m
	# We build the binaries becuase 'go run' won't consistently pass signals to our program.
	$(FIRSTGOPATH)/bin/setup-cloud-scoot --strategy local.local run scootapi run_smoke_test --num_jobs 10 --timeout 1m $(TRAVIS_FILTER)

recoverytest: install
	# Some overlap with swarmtest but focuses on sagalog recovery vs worker/checkout correctness.
	# We build the binaries becuase 'go run' won't consistently pass signals to our program.
	# Ignore output here to reduce travis log size. Swarmtest is more important and that still logs.
	$(FIRSTGOPATH)/bin/recoverytest &>/dev/null

integrationtest: install
	# Integration test with some overlap with other standalone tests, but utilizes client binaries
	$(FIRSTGOPATH)/bin/scoot-integration &>/dev/null
	$(FIRSTGOPATH)/bin/bazel-integration &>/dev/null

############## cleanup

clean-mockgen:
	find . -name \*_mock.go -type f -delete

clean-data:
	rm -rf ./.scootdata/*

clean-go:
	go clean ./...

clean: clean-data clean-go

############## code gen for mocks, bindata configs, thrift, and protoc

generate:
	go generate ./...

thrift-worker-go:
	# Create generated code in github.com/twitter/scoot/workerapi/gen-go/... from worker.thrift
	cd workerapi && rm -rf gen-go && thrift -I ../bazel/execution/bazelapi/ --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift worker.thrift && cd ..
	rm -rf workerapi/gen-go/worker/worker-remote/

thrift-sched-go:
	# Create generated code in github.com/twitter/scoot/sched/gen-go/... from sched.thrift
	cd sched && rm -rf gen-go && thrift -I ../bazel/execution/bazelapi/ --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift sched.thrift && cd ..

thrift-scoot-go:
	# Create generated code in github.com/twitter/scoot/scootapi/gen-go/... from scoot.thrift
	cd scootapi && rm -rf gen-go && thrift -I ../bazel/execution/bazelapi/ --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift && cd ..
	rm -rf scootapi/gen-go/scoot/cloud_scoot-remote/

thrift-bazel-go:
	# Create generated code in github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/... from bazel.thrift
	cd bazel/execution/bazelapi && rm -rf gen-go && thrift --gen go:package_prefix=github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift bazel.thrift && cd ..

thrift-go: thrift-sched-go thrift-scoot-go thrift-worker-go thrift-bazel-go

thrift: thrift-go

bazel-proto:
	# see bazel/remoteexecution/README.md

############## top-level dev-fullbuild, travis targets

dev-fullbuild: dev-dependencies generate test-all

travis: fs_util recoverytest swarmtest integrationtest test-all clean-data
